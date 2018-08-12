release:
	mkdir -p build/release; cd build/release; cmake ../../src -DCMAKE_BUILD_TYPE=Release; make ${args}; cd -;

debug:
	mkdir -p build/debug; cd build/debug; cmake ../../src -DCMAKE_BUILD_TYPE=Debug; make ${args}; cd -;

lint:
	./scripts/cpplint.py src/*/*h src/*/*cc src/sim/*/*cc src/md/*/*h src/md/*/*cc src/algo/*/*h src/algo/*/*cc

fmt:
	clang-format -style=Google -i src/*/*h src/*/*cc src/sim/*/*cc src/md/*/*h src/md/*/*cc src/algo/*/*h src/algo/*/*cc

clean:
	rm -rf build;
