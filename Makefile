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

dialyzer: compile
	@dialyzer -Wno_return -c ebin
