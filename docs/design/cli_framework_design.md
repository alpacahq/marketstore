# Marketstore CLI Framework Design

Date: 08/03/2018

Author: Michael Ackley <ackleymi@gmail.com>

## Goal
Unifies the project with one entrypoint, with database user interactions now limited to a set of CLI commands.

## Summary
* The whole go project (minus any plugins) is built as a single application distribution.
* The one-off tools walDebugger and integrityChecker under the previous cmd/tools directory will be migrated to the new `marketstore tool <toolname> [flags]` command.
* The functionality in the `mkts` application will be migrated to the new `marketstore connect [flags]` command.
* The functionality in the main `marketstore` application will be migrated to the new `marketstore start [flags]` command.
* A new command called `init` will create a new mkts.yml file in the current directory.
* The `mtks` application used to be two giant files. It is now refactored out logically into a few packages and files.
* runtest.sh and the example data (that had existing bugs in it) are now deprecated, TBD here..
* Detailed application help information a user might need is easily accessible with every command using either `marketstore help <command>` or `marketstore <command> --help`.

## Performance
No performance impacts are expected. Functionality is just reorganized regarding how users execute tools or sql commands or start a db server. This refactor only aims to maximize usability and allow new users to easily navigate and use the main features of the database through the command-line.

## Design Overview
github.com/spf13/cobra is the cli commander library that provides a framework for building modern cli applications. The main concept is, a root command, in this case `marketstore` can have subcommands and flags. Those subcommands can also have subcommands and flags, effectively building a nice tree of logic and commands. Commands can also accept non-flag arguments where necessary.

A few nice examples of this design (in production) are popular command-line applications like docker, kubernetes, hugo, cockroachdb, influxdb, etc.

It is really easy to see how the commands and laid out in practice- simply run `marketstore`, nothing else, and it will print out the list of commands and a bunch of useful help information. It will look something like this-
```
Usage:
  marketstore [flags]
  marketstore [command]

Available Commands:
  connect     Open an interactive session with an existing marketstore database
  help        Help about any command
  init        Creates a new mkts.yml file
  start       Start a marketstore database server
  tool        Execute the specified tool

Flags:
  -h, --help      help for marketstore
  -v, --version   show the version info and exit

Use "marketstore [command] --help" for more information about a command.
```

### Some features of this framework
* All errors are handled in an idiomatic go fashion through return values and handled gracefully.
* Flags are fully POSIX-compliant flags (including short & long versions).
* Flags are highly configurable.
* Commands can be aliased or hidden.
* Help information is easy to define and is formatted nicely by default.
* Easily generate bash autocomplete or man pages.

### Structure
The file `marketstore.go` in the project's top-level directory is effectively the entrypoint. It is, by design, really compact.
```go
package main

import (
	"os"

	"github.com/alpacahq/marketstore/cmd"
)

// Builds the command hierarchy and parses statements.
func main() {
	err := cmd.Execute()
	// Errors are already handled by the framework.
	if err != nil {
		os.Exit(0)
	}
}
```

This function bootstraps the command hierarchy that exists in the cmd/ directory, then parses the arguments and executes the commands.

### How commands are organized
You can find where commands are defined using this map of the relevant directory structure.
```
marketstore/
  marketstore.go
  cmd/
    main.go
    connect/
      main.go
		create/
      main.go
    start/
      main.go
    tool/
      main.go
      wal/
        main.go
      integrity/
        main.go
```

## Commands Overview
Marketstore commands allow users to start/stop a database instance, execute sql commands, and run debug tools on the db files. Help and version information is provided on a global command level and for each subcommand and flag. Help information is displayed when commands are misspelled or used improperly. Flag and argument validation is provided by default.

In this implementation, marketstore commands are all represented as pointer instances of the `cobra.Command` struct and are defined using the struct's public collection of fields. From the `cobra.Command` godoc -
```go
// Command is just that, a command for your application.
// E.g.  'go run ...' - 'run' is the command. Cobra requires
// you to define the usage and description as part of your command
// definition to ensure usability.
```

### Benefits of cobra.Command structs
Ability for future customization/modification is considered. The framework is designed to be flexible with optional `cobra.Command` fields like
```go
// Aliases is an array of aliases that can be used instead of the first word in Use.
Aliases []string

// SuggestFor is an array of command names for which this command will be suggested -
// similar to aliases but only suggests.
SuggestFor []string

// ArgAliases is List of aliases for ValidArgs.
// These are not suggested to the user in the bash completion,
// but accepted if entered manually.
ArgAliases []string

// Annotations are key/value pairs that can be used by applications to identify or
// group commands.
Annotations map[string]string
```

The framework also allows for backwards compatibility with optional `cobra.Command` fields like
```go
// Deprecated defines, if this command is deprecated and should print this string when used.
Deprecated string

// Hidden defines, if this command is hidden and should NOT show up in the list of available commands.
Hidden bool

// Version defines the version for this command. If this value is non-empty and the command does not
// define a "version" flag, a "version" boolean flag will be added to the command and, if specified,
// will print content of the "Version" variable.
Version string
```

## Command Specifications

### Start
This is the most important command. It starts a new `marketstore` db instance. This functionality was previously the sole purpose of the old `marketstore` application.

#### Example
`marketstore start --config <path>`

#### Flags
Name | Shortcut | Purpose | Required | Default
--- | --- | --- | --- | ---
--config | -c | specifying the path of the config file | no | ./mkts.yml


### Connect
This command opens an interactive session with an existing marketstore database where a user can execute sql statements (queries) and examine database diagnostics. This is accomplished by providing the command with either a location of on-disk marketstore database files OR a network address of a running marketstore server.

#### Example
`marketstore connect --url <address>`

#### Flags
Name | Shortcut | Purpose | Required | Default
--- | --- | --- | --- | ---
--url | -u | specifying the address to database instance | no | none
--dir | -d | specifying the path of database files | no | none


### Init
A super useful command for getting a new database up and running. It creates a new `mkts.yml` file and an empty data/ directory in the current directory. This config file is populated with defaults and examples.

#### Example
`marketstore init`

#### Flags
N/A


### Tool - WAL
The WAL file debugging tool can now be executed as a command.

#### Example
`marketstore tool wal --file <path>`

#### Flags
Name | Shortcut | Purpose | Required | Default
--- | --- | --- | --- | ---
--file | -f | specifying the path of the WAL file | yes | none


### Tool - Integrity
The integrity checker debugging tool can now be executed as a command.

#### Example
`marketstore tool integrity --dir <path> --fix --parallel`

#### Flags
Name | Shortcut | Purpose | Required | Default
--- | --- | --- | --- | ---
--dir | -d | specifying the directory of the db files | yes | none
--chunks | none | number of checksum chunks per file, excluding the header | no | none
--parallel | none | evaluate checksums in parallel | no | none
--fix | none | resolve any header problems the scan identifies | no | none
--monthStart | none | set the lower bound of the evaluation | no | none
--monthEnd | none | set the upper bound of the evaluation | no | none
--yearStart | none | set the lower bound of the evaluation | no | none
--yearEnd | none | set the upper bound of the evaluation | no | none
