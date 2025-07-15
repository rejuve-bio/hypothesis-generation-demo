:- use_module(library(mcintyre)).
:- mc.
:- begin_lpad.

coin(fair):0.5; coin(biased):0.5.

toss(C,heads):0.5; toss(C,tails):0.5 :- coin(fair).
toss(C,heads):0.8; toss(C,tails):0.2 :- coin(biased).

:- end_lpad.