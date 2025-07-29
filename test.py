import unittest


try:
  loader = unittest.TestLoader()
  suite = loader.discover(start_dir="tests")
  # suite = unittest.TestLoader().loadTestsFromName("tests.test_safe_passage.TestSafePassage")
  runner = unittest.TextTestRunner()
  result = runner.run(suite)
  if not result.wasSuccessful():
    exit(1)

except Exception as e:
  print(e)
  exit(1)
