:- use_module(library(optparse)).
:- use_module(library(interpolate)).
:- initialization(main).

% Define the main entry point
main :-
    current_prolog_flag(argv, Argv),
    parse_options(Argv, Options),
    format('Options: ~w~n', [Options]),
    member(path(Dir), Options),
    member(hook(Hook), Options),
    member(nodes(Nodes), Options),
    member(edges(Edges), Options),
    compile_with_time(Dir, Hook, Nodes, Edges),
    halt.

compile_with_time(Dir, Hook, Nodes, Edges) :-
    consult(Hook),
    format("Compiling files in ~w...~n", [Dir]),
    (Nodes -> 
        directory_file_path(Dir, nodes, NodesFile),
        absolute_file_name(NodesFile, AbsNodesFile), 
        format("Compiling nodes file ~w ... ~n", [AbsNodesFile]),
        qcompile(AbsNodesFile)
        ; true),
    (Edges -> 
        directory_file_path(Dir, edges, EdgesFile),
        absolute_file_name(EdgesFile, AbsEdgesFile), 
        format("Compiling edges file ~w ...~n", [AbsEdgesFile]),
        qcompile(AbsEdgesFile)
        ; true),
    retractall(seen_predicate(_, _, _)),
    format("Compilation Done.~n").

% Define the options specification
opt_spec([
    [opt(path), type(atom), shortflags([p]), longflags([path]), 
     help('Path to the directory')],
    [opt(hook), type(atom), shortflags([h]), longflags([hook]), 
     help('Path to the hook.pl')],
    [opt(nodes), type(boolean), shortflags([n]), longflags([nodes]), 
     default(false), help('Load nodes.pl file')],
    [opt(edges), type(boolean), shortflags([e]), longflags([edges]), 
     default(false), help('Load edges.pl file')]

]).

% Parse the command line options
parse_options(Argv, Options) :-
    opt_spec(Spec),
    opt_parse(Spec, Argv, Options, _).
