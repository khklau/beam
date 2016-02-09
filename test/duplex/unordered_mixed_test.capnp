@0xefacebc97244f461;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("beam::duplex::unordered_mixed");

struct ReliableMsg
{
    value @0 :Text;
}

struct UnreliableMsg
{
    value @0 :UInt32;
}
