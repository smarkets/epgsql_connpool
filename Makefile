.PHONY: deps

all: deps compile
test: deps eunit

compile:
	./rebar compile

deps:
	./rebar get-deps

clean:
	./rebar clean

distclean: clean
	./rebar delete-deps

eunit:
	./rebar skip_deps=true eunit

docs:
	./rebar skip_deps=true doc
	@cp -R apps/epgsql/doc doc/epgsql
	@cp -R apps/epgsql_pool/doc doc/epgsql_pool

dialyzer: compile
	@dialyzer -Wno_return -c apps/epgsql_pool/ebin
