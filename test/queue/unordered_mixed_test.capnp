@0xebf7e53bbbdfa9c2;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("beam::queue::unordered_mixed");

struct ReliableMsg
{
    value @0 :Text;
}

struct UnreliableMsg
{
    value @0 :UInt32;
}
