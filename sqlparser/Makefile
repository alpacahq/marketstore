all: generateParser generateCode

generateParser: clean
	java -jar /usr/local/lib/antlr-4.9.2-complete.jar -o . -no-visitor -listener -encoding utf-8 -Dlanguage=Go -lib parser parser/SQLBase.g4
	#antlr4 -o . -no-visitor -listener -encoding utf-8 -Dlanguage=Go -lib parser parser/SQLBase.g4

generateCode: dummy-setup
	go generate

dummy-setup:
	@echo "package parser" > parser/dummy.go

clean:
	rm -f parser/*.go parser/*.tokens parser/*.java *_string.go
