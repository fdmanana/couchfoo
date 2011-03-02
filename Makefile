EBIN_DIR = ebin
SRC_DIR = src
TESTS_DIR = tests
TEST_TMP_DIR = tmp
ERLC = erlc

all: clean couchfoo

couchfoo: ebin
	cd ebin && zip -9 ../ebin.zip *.beam && cd ..
	echo '#!/usr/bin/env escript' > $@
	echo "%%! -smp enable -escript main $@" >> $@
	cat ebin.zip >> $@
	chmod +x $@
	rm -f ebin.zip

ebin: $(SRC_DIR)/*.* $(TESTS_DIR)/*.*
	mkdir -p $(EBIN_DIR)
	$(ERLC) -o $(EBIN_DIR) -I $(SRC_DIR) -I $(TESTS_DIR) $(SRC_DIR)/*.erl $(TESTS_DIR)/*.erl

clean:
	rm -fr $(EBIN_DIR) $(TEST_TMP_DIR)
	rm -f ebin.zip couchfoo

test: ebin
	mkdir -p $(TEST_TMP_DIR)
	rm -fr $(TEST_TMP_DIR)/*
	./support/run_tests.escript $(EBIN_DIR)
	rm -fr $(TEST_TMP_DIR)/*