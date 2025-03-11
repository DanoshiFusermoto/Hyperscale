/**
 * Simple faucet
 */
const Context = Java.type("org.radix.hyperscale.Context");
const ComputeKey = Java.type("org.radix.hyperscale.crypto.ComputeKey");
const StateAddress = Java.type("org.radix.hyperscale.ledger.StateAddress");
const StateLockMode = Java.type("org.radix.hyperscale.ledger.StateLockMode");
const UInt256 = Java.type("org.radix.hyperscale.utils.UInt256");

function constructor(token, quantity)
{
	if (token === undefined)
		throw "Token symbol is undefined";

	if (quantity === undefined)
		throw "Quantity is undefined";
	
	// The token symbol being distributed
	_self.set("token", token);
	
	// Quantity of tokens to give out each time
	_self.set("quantity", quantity);
	
	// TODO store this temporarily as need a JS module to calculate it
	_self.set("vault", ":i:"+_self.identity().toString());
	
	// A list of addresses which have already interacted
	// TODO might need to find a more efficient method here such as a bloom filter as could get heavy!
	_self.set("distributions", new Array());
}

function lock()
{
	var vaultStateAddress = new StateAddress("vault", _self.identity());
	_self.lock(vaultStateAddress, StateLockMode.WRITE);
}

function gimme(requester)
{
	var distributions = _self.get("distributions");
	if (distributions.contains(requester))
		throw "Has already received a token distribution";
	
	var token = _self.get("token");
	var quantity = _self.get("quantity");
	var selfVault = _self.vault(_self.identity());
	var requesterVault = _self.vault(requester);
	
	var faucetBucket = _self.bucket("faucet");
	selfVault.take(token, quantity, faucetBucket);
	requesterVault.put(token, quantity, faucetBucket);
	distributions.add(requester);
	
	_self.set("distributions", distributions);
}
