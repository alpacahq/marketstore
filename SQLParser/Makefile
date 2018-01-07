generateParser: clean generateCode
	antlr4 -o . -no-visitor -listener -encoding utf-8 -Dlanguage=Go -lib parser parser/SQLBase.g4

generateCode:
	go generate

clean:
	rm -f parser/*.go parser/*.tokens parser/*.java
