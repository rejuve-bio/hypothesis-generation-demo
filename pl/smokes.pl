begin(model(1)).
neg(smokes(2)).
end(model(1)).

begin(model(2)).
smokes(4).
end(model(2)).

begin(model(3)).
neg(influences(1, 2)).
end(model(3)).

begin(model(4)).
neg(influences(4, 2)).
end(model(4)).

begin(model(5)).
influences(2, 3).
end(model(5)).

begin(model(6)).
stress(1).
end(model(6)).
