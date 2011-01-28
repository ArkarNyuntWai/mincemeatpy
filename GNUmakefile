#
# GNU 'make' file
# 

.PHONY: all test

all:

# Only run tests in this directory.
test:
	@py.test --version || echo "py.test not found; run 'sudo easy_install pytest'?"
	py.test --capture=no *_test.py

# Run only tests with a prefix containing the target string, eg test-blah
test-%:
	py.test --capture=no *$*_test.py

#
# Target to allow the printing of 'make' variables, eg:
#
#     make print-CXXFLAGS
#
print-%:
	@echo $* = $($*)
	@echo $*\'s origin is $(origin $*)
