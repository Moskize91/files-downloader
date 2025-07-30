from typing import TypeVar


T = TypeVar("T")

def list_safe_remove(lst: list[T], item: T) -> T | None:
  index = -1
  for i, elem in enumerate(lst):
    if elem is item:
      index = i
      break
  if index >= 0:
    return lst.pop(index)