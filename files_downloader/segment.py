from dataclasses import dataclass
from typing import Callable, Iterable
from threading import Lock, Event
from .errors import InterruptionError


@dataclass
class SegmentDescription:
  offset: int
  length: int
  completed_length: int

# thread safe
class Segment:
  def __init__(
        self,
        offset: int,
        length: int,
        completed_length: int,
        send_back: Callable[["Segment"], None],
      ) -> None:

    self._offset: int = offset
    self._length: int = length
    self._locked_length: int = completed_length
    self._completed_length: int = completed_length
    self._send_back: Callable[["Segment"], None] = send_back
    self._lock: Lock = Lock()
    self._interrupted: bool = False

  @property
  def offset(self) -> int:
    return self._offset

  @property
  def length(self) -> int:
    return self._length

  @property
  def completed_length(self) -> int:
    return self._completed_length

  @property
  def is_completed(self) -> bool:
    with self._lock:
      return self._completed_length >= self._length

  def lock(self, delta_length: int) -> int:
    with self._lock:
      if self._interrupted:
        raise InterruptionError()
      origin_locked_length = self._locked_length
      self._locked_length = min(origin_locked_length + delta_length, self._length)
      return self._locked_length - origin_locked_length

  def submit(self, delta_length: int) -> None:
    with self._lock:
      next_completed_length = self._completed_length + delta_length
      if next_completed_length > self._locked_length:
        raise ValueError("cannot submit more than locked length")
      self._completed_length = next_completed_length

  def complete(self) -> None:
    with self._lock:
      self._locked_length = self._length
      self._completed_length = self._length

  def interrupt(self) -> None:
    with self._lock:
      self._interrupted = True

  def dispose(self) -> None:
    with self._lock:
      self._send_back(self)

@dataclass
class _Node:
  taken: bool
  done: bool
  segment: Segment

# not thread safe
class Serial:
  def __init__(
        self,
        length: int,
        min_segment_length: int,
        descriptions: Iterable[SegmentDescription] | None = None,
      ) -> None:

    assert length > 0, "length must be greater than 0"
    assert min_segment_length > 1, "min_segment_length must be greater than 1"
    self._length: int = length
    self._min_segment_length: int = min_segment_length
    self._nodes_lock: Lock = Lock()
    self._nodes: list[_Node] = []
    self._did_dispose: bool = False
    self._wait_interrupted_segments: int = 0
    self._wait_all_interrupted: Event = Event()

    if descriptions is None:
      self._nodes.append(_Node(
        taken=False,
        done=False,
        segment=Segment(
          offset=0,
          length=length,
          completed_length=0,
          send_back=self._receive_segment,
        ),
      ))
    else:
      offset: int = 0
      for description in sorted(list(descriptions), key=lambda d: d.offset):
        if description.offset != offset:
          raise ValueError(f"except segment offset to be continuous: wanna {offset} but got {description.offset}")
        if description.completed_length > description.length:
          raise ValueError(f"segment completed length {description.completed_length} cannot be greater than its length {description.length}")
        self._nodes.append(_Node(
          taken=False,
          done=description.completed_length >= description.length,
          segment=Segment(
            offset=description.offset,
            length=description.length,
            completed_length=description.completed_length,
            send_back=self._receive_segment,
          ),
        ))
        offset += description.length
        if offset > length:
          raise ValueError(f"segment's tail {offset} exceeds total length {length}")
      if offset != length:
        raise ValueError(f"end of segments {offset} does not match total length {length}")

  @property
  def length(self) -> int:
    return self._length

  @property
  def is_completed(self) -> bool:
    with self._nodes_lock:
      return all(node.done for node in self._nodes)

  def snapshot(self) -> list[SegmentDescription]:
    with self._nodes_lock:
      descriptions = [
        SegmentDescription(
          offset=node.segment.offset,
          length=node.segment.length,
          completed_length=node.segment.completed_length,
        )
        for node in self._nodes
      ]
    descriptions.sort(key=lambda d: d.offset)

    return descriptions

  def interrupt(self) -> None:
    self._interrupt_taken_segments()

  def dispose(self) -> None:
    wait_count: int = self._interrupt_taken_segments()
    if wait_count > 0:
      self._wait_all_interrupted.wait()

  def _interrupt_taken_segments(self) -> int:
    wait_count: int = 0
    with self._nodes_lock:
      if self._did_dispose:
        return 0
      for node in self._nodes:
        if node.taken:
          node.segment.interrupt()
          wait_count += 1
      self._wait_interrupted_segments = wait_count
      self._wait_all_interrupted.clear()
      self._did_dispose = True
    return wait_count

  def pop_segment(self) -> Segment | None:
    if self._did_dispose:
      return None

    with self._nodes_lock:
      useable_sorted_keys: list[tuple[int, int, int]] = []
      for node_index, node in enumerate(self._nodes):
        if node.done:
          continue
        taken_index: int = 0
        segment = node.segment
        remain_length = segment.length - segment._locked_length

        if node.taken:
          taken_index = 1
          if remain_length < 2 * self._min_segment_length:
            # 低于此值，若切割它必然产生一个小于 min_segment_length 的片段
            continue
        useable_sorted_keys.append((taken_index, -remain_length, node_index))

      for _, _, node_index in sorted(useable_sorted_keys):
        node = self._nodes[node_index]
        segment = node.segment
        if not node.taken:
          node.taken = True
          return segment

        with segment._lock:
          cutted_segment = self._try_to_cut_segment(segment)
          if cutted_segment:
            return cutted_segment

      return None

  # all locks will be locked by the caller
  def _try_to_cut_segment(self, segment: Segment) -> Segment | None:
    remain_length = segment.length - segment._locked_length
    cutted_length = remain_length // 2

    if cutted_length < self._min_segment_length:
      # 两次上锁之间，状态可能发生变化，需要再校验一次
      return None

    segment._length -= cutted_length
    cutted_segment = Segment(
      offset=segment._offset + segment._length,
      length=cutted_length,
      completed_length=0,
      send_back=self._receive_segment,
    )
    self._nodes.append(_Node(
      taken=True,
      done=False,
      segment=cutted_segment,
    ))
    return cutted_segment

  def _receive_segment(self, segment: Segment) -> None:
    with self._nodes_lock:
      received_node: _Node | None = None
      for node in self._nodes:
        if node.segment == segment:
          received_node = node
          break

      if received_node is None:
        return
      received_node.taken = False
      if segment.completed_length >= segment.length:
        received_node.done = True

      if self._did_dispose:
        self._wait_interrupted_segments -= 1
        if self._wait_interrupted_segments <= 0:
          self._wait_all_interrupted.set()