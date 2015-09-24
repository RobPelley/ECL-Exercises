IMPORT Python;

INTEGER addone(INTEGER p) := EMBED(Python)

if p < 10:
	return p+1
else:
	return 0

ENDEMBED;

i := addone(6);

OUTPUT(i, NAMED('Result'));

