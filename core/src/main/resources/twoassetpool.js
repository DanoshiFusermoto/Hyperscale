/**
 * Simple two asset swap pool
 */
const Hash = Java.type("org.radix.hyperscale.crypto.Hash");
const StateAddress = Java.type("org.radix.hyperscale.ledger.StateAddress");
const UInt256 = Java.type("org.radix.hyperscale.utils.UInt256");
const Currency = Java.type("org.radix.hyperscale.utils.Currency");

const BOOTSTRAP = true;
const DEBUG = false;
const INFO = true;
 
function constructor(tokenA, tokenB, bootstrapA, bootstrapB)
{
	if (tokenA === undefined)
		throw "Token A symbol is undefined";
	
	if (tokenB === undefined)
		throw "Token B symbol is undefined";
	
	if (bootstrapA === undefined)
		throw "Token A bootstrap amount is undefined";

	if (bootstrapB === undefined)
		throw "Token B bootstrap amount is undefined";

	if (tokenA == tokenB)
		throw "Token references are the same";
	
	_self.set("token_a", tokenA);
	_self.set("token_b", tokenB);
	
	// TODO store this temporarily as need a JS module to calculate it
	_self.set("vault", ":i:"+_self.identity().toString());

	if (BOOTSTRAP)
	{
		_self.bucket("bootstrap").put(tokenA, bootstrapA);
		_self.bucket("bootstrap").put(tokenB, bootstrapB);
		_self.vault(_self.identity()).put(tokenA, bootstrapA, _self.bucket("bootstrap"));
		_self.vault(_self.identity()).put(tokenB, bootstrapB, _self.bucket("bootstrap"));
		
		var liquidity = mintLPToken(bootstrapA, bootstrapB);
		_self.set("total_liquidity", liquidity);
	}
	else
		_self.set("total_liquidity", UInt256.ZERO);
}

function quote(amountA, reserveA, reserveB)
{
	if (reserveA.equals(UInt256.ZERO) || reserveB.equals(UInt256.ZERO))
		throw "Insufficient liquidity";

    return amountA.multiply(reserveB).divide(reserveA);
}

function addLiquidity(amountA, amountB, provider)
{
	var tokenA = _self.get("token_a");
	var tokenB = _self.get("token_b");
	
	if (amountA.equals(UInt256.ZERO))
		throw "Token amount for "+tokenA+" is zero";
	
	if (amountB.equals(UInt256.ZERO))
		throw "Token amount for "+tokenB+" is zero";

	var selfVault = _self.vault(_self.identity());
	var providerVault = _self.vault(provider);

	var reserveA = selfVault.balance(tokenA);
	var reserveB = selfVault.balance(tokenB);
	
	if (DEBUG) 
		console.log("Reserves (pre-liquidity): "+selfVault.balance(tokenA)+" "+tokenA+" / "+selfVault.balance(tokenB)+" "+tokenB);

    if (reserveA.equals(UInt256.ZERO) === false && reserveB.equals(UInt256.ZERO) === false) 
	{
        var amountBOptimal = quote(amountA, reserveA, reserveB);
        if (amountBOptimal <= amountB) 
		{
            amountB = amountBOptimal;
        } 
		else 
		{
			var amountAOptimal = quote(amountB, reserveB, reserveA);
			if (amountAOptimal <= amountA)
				throw "Can not add unbalanced liquidity "+amountA+" "+tokenA+" <-> "+amountB+" "+tokenB;

            amountA = amountAOptimal;
        }
    }
	
	var liquidity = mintLPToken(amountA, amountB);
	var liquidityBucket = _self.bucket("liquidity");
	providerVault.take(tokenA, amountA, liquidityBucket);
	providerVault.take(tokenB, amountB, liquidityBucket);
	selfVault.put(tokenA, amountA, liquidityBucket);
	selfVault.put(tokenB, amountB, liquidityBucket);

	liquidityBucket.put(_self.address().getHash(), liquidity);
	providerVault.put(_self.address().getHash(), liquidity, liquidityBucket);
	
	if (DEBUG) 
		console.log("Reserves (post-liquidity): "+selfVault.balance(tokenA)+" "+tokenA+" / "+selfVault.balance(tokenB)+" "+tokenB);
}

function mintLPToken(amountA, amountB)
{
	var selfVault = _self.vault(_self.identity());
	var tokenA = _self.get("token_a");
	var tokenB = _self.get("token_b");
	var reserveA = selfVault.balance(tokenA);
	var reserveB = selfVault.balance(tokenB);

	var liquidity = UInt256.ZERO;
    var totalLiquidity = _self.get("total_liquidity", UInt256.ZERO);
	
    if (totalLiquidity.equals(UInt256.ZERO)) 
        liquidity = amountA.multiply(amountB).isqrt();
	else 
	{
		var sharesA = amountA.multiply(totalLiquidity).divide(reserveA);
		var sharesB = amountB.multiply(totalLiquidity).divide(reserveB);
        liquidity = sharesA;
		if (sharesB.compareTo(sharesA) < 0)
			liquidity = sharesB;
    }

	_self.set("total_liquidity", totalLiquidity.add(liquidity));
	
	return liquidity;
}

function removeLiquidity(liquidity, provider)
{
	if (liquidity.equals(UInt256.ZERO))
		throw "Liquidity amount "+liquidity+" is zero";

	var providerVault = _self.vault(provider);
	
	if (providerVault.balance(_self.address().getHash()).compareTo(liquidity) < 0)
		throw "Provider does not have sufficient liquidity "+liquidity;
	
	var selfVault = _self.vault(_self.identity());
	var tokenA = _self.get("token_a");
	var tokenB = _self.get("token_b");
	var reserveA = selfVault.balance(tokenA);
	var reserveB = selfVault.balance(tokenB);
    var totalLiquidity = _self.get("total_liquidity");
	
	if (DEBUG) 
		console.log("Reserves (pre-liquidity): "+selfVault.balance(tokenA)+" "+tokenA+" / "+selfVault.balance(tokenB)+" "+tokenB);
	
    var amountA = liquidity.multiply(reserveA).divide(totalLiquidity);
    var amountB = liquidity.multiply(reserveB).divide(totalLiquidity);

	if (amountA.equals(UInt256.ZERO))
		throw "Token liquidity amount for "+tokenA+" is zero";
	
	if (amountB.equals(UInt256.ZERO))
		throw "Token liquidity amount for "+tokenB+" is zero";

	var liquidityBucket = _self.bucket("liquidity");

	selfVault.take(tokenA, amountA, liquidityBucket);
	selfVault.take(tokenB, amountB, liquidityBucket);
	providerVault.put(tokenA, amountA, liquidityBucket);
	providerVault.put(tokenB, amountB, liquidityBucket);
	providerVault.take(_self.address().getHash(), liquidity, liquidityBucket);
	liquidityBucket.take(_self.address().getHash(), liquidity);

	_self.set("total_liquidity", totalLiquidity.subtract(liquidity));
	
	if (DEBUG) 
		console.log("Reserves (post-liquidity): "+selfVault.balance(tokenA)+" "+tokenA+" / "+selfVault.balance(tokenB)+" "+tokenB);
}

function swap(token, amount, to)
{
	var tokenA = _self.get("token_a");
	var tokenB = _self.get("token_b");
	if (token !== tokenA && token !== tokenB)
		throw "Token "+token+" is not supported by pool";
	
	var tokenIn = token;
	var tokenOut = token === tokenA ? tokenB : tokenA
	if (DEBUG) 
		console.log("Swapping "+amount+" "+tokenIn+" -> "+tokenOut);
	
	var selfVault = _self.vault(_self.identity());
	var toVault = _self.vault(to);
	if (DEBUG) 
	{
		console.log("Reserves (pre-swap): "+selfVault.balance(tokenA)+" "+tokenA+" / "+selfVault.balance(tokenB)+" "+tokenB);
		var price = Currency.price(selfVault.balance(tokenA), selfVault.balance(tokenB));
		console.log("Price (pre-swap): "+price+" "+tokenB);
	}

	var swapBucket = _self.bucket("swap");
	var reserveIn = selfVault.balance(tokenIn);
	var reserveOut = selfVault.balance(tokenOut);
	
	var amountInWithFee = amount.multiply(UInt256.from(997));
	if (DEBUG) 
		console.log("Amount to swap minus fee: "+amountInWithFee.divide(UInt256.from(1000))+" "+tokenIn);
	
    var numerator = amountInWithFee.multiply(reserveOut);
    var denominator = reserveIn.multiply(UInt256.THOUSAND).add(amountInWithFee);
    var amountOut = numerator.divide(denominator);
	if (DEBUG) 
		console.log("Receiving: "+amountOut+" "+tokenOut);
	
	toVault.take(tokenIn, amount, swapBucket);
	selfVault.take(tokenOut, amountOut, swapBucket);
	selfVault.put(tokenIn, amount, swapBucket);
	toVault.put(tokenOut, amountOut, swapBucket);
	
	var price = Currency.price(selfVault.balance(tokenA), selfVault.balance(tokenB));
	_self.set("price", price.toString());
	_self.set("counter", _self.get("counter")+1);
	
	if (DEBUG) 
	{
		console.log("Reserves (post-swap): "+selfVault.balance(tokenA)+" "+tokenA+" / "+selfVault.balance(tokenB)+" "+tokenB);
		console.log("Price (post-swap): "+price+" "+tokenB);
		console.log("");
	}
}