@0xfa63524727abb135;
using Go = import "/go.capnp";
$Go.package("myPackage");
$Go.import("myPackage");

struct Car {
  make @0 :Text;
  model @1 :Text;
}
