:- use_module(library(janus)).

run_python_script :-
    janus:py_import('mp_example', []),
    janus:py_call(mp_example:main(), _).

:- initialization(run_python_script).
