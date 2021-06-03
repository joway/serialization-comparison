using Go = import "/go.capnp";

@0xe9e41470b2c8e533;
$Go.package("user");
$Go.import("user");

struct User {
  name  @0 :Text;
  email @1 :Text;
}
