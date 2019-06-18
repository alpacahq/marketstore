# System level tests for Marketstore


These tests are initially implemented test tick support in the published Marketstore docker image.

## Tests
### All Tests
- Command
```
make -C tests/integ test
```
including Connection Test, CSV Import Test, TICK Test and others

### Connection Test
- Command
```
make -C tests/integ connect
```

### CSV Import Test
- Command
```
make -C tests/integ test_import_csv
```
import of tick data using csv files import


### TICK read/write test

- Command (all tests including TICK read/write )
```
make -C tests/integ test
```

- download and run latest marketstore docker image
- writing and querying ticks with pymarketstore client


- Status (01/19/2019): 
not checking Nanoseconds column values since precision is not enough