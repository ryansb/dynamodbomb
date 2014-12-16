# DynamoDBomb

DynamoDBomb is built on top of Crowdmob's dynamo support in [goamz][goamz]
which is itself a fork of [the original goamz][lpgoamz]. This is *not*
compatible with github.com/crowdmob/goamz/dynamodb, but instead aims to be more
developer-friendly and smooth out some of the rough API edges.

## Licensing

Just like crowdmob, I'm licensing this under the LGPLv3, see LICENSE.txt for
more details.

## Credits

Without the crowdmob and goamz teams this project wouldn't exist, so give due
credit to the Canonical team and [crowdmob/goamz contributors][goamzcontrib],
and due bug reports to [me](https://github.com/ryansb/dynamodbomb/issues)

## Running the Tests

### Against DynamoDB Local

To download and launch DynamoDB local:

```sh
$ make
```

To test:

```sh
$ go test -v -amazon
```

### Against Real DynamoDB in US-EAST

_WARNING_: Some dangerous operations such as `DeleteTable` will be performed during the tests. Please be careful.

To test:

```sh
$ go test -v -amazon -local=false
```

_Note_: Running tests against real DynamoDB will take several minutes.

[goamz]: https://github.com/crowdmob/goamz
[lpgoamz]: https://wiki.ubuntu.com/goamz
[goamzcontrib]: https://github.com/crowdmob/goamz/contributors
