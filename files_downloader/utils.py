from typing import TypeVar


T = TypeVar("T")

def list_safe_remove(lst: list[T], item: T) -> None:
  index = -1
  for i, elem in enumerate(lst):
    if elem is item:
      index = i
      break
  if index >= 0:
    lst.pop(index)