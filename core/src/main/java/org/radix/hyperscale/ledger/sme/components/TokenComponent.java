package org.radix.hyperscale.ledger.sme.components;

import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.exceptions.ValidationException;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.StateContext;
import org.radix.hyperscale.ledger.primitives.Blob;
import org.radix.hyperscale.ledger.sme.Component;
import org.radix.hyperscale.ledger.sme.NativeComponent;
import org.radix.hyperscale.ledger.sme.StateMachine;
import org.radix.hyperscale.utils.Strings;
import org.radix.hyperscale.utils.UInt256;

@StateContext("token")
public class TokenComponent extends NativeComponent 
{
	static
	{
		Component.registerNative(TokenComponent.class);
	}
	
	public TokenComponent(StateMachine stateMachine)
	{
		super(stateMachine, TokenComponent.class);
	}
	
	public void create(String symbol, String description, Identity authority) throws ValidationException
	{
		create(symbol, description, null, authority);
	}
	
	public void create(String symbol, String description, Blob icon, Identity authority)
	{
		symbol = Strings.toLowerCase(symbol);
		StateAddress tokenStateAddress = StateAddress.from(context(), Hash.valueOf(symbol));
		assertCreate(tokenStateAddress, authority);
			
		set(tokenStateAddress, "symbol", symbol);
		set(tokenStateAddress, "description", description);
		
		if (icon != null)
			set(tokenStateAddress, "icon", icon.getHash());
		
		associate(tokenStateAddress, Hash.valueOf(context()));
		associate(tokenStateAddress, authority);
	}

	public void mint(String symbol, UInt256 quantity, Identity receiver, Identity authority) throws ValidationException
	{
		symbol = Strings.toLowerCase(symbol);
		StateAddress tokenStateAddress = StateAddress.from(context(), Hash.valueOf(symbol));
		assertExists(tokenStateAddress);
		
		UInt256 currentSupply = get(tokenStateAddress, "supply", UInt256.ZERO);
		bucket("mint").put(symbol, quantity);
		set(tokenStateAddress, "supply", currentSupply.add(quantity));
		vault(receiver).put(symbol, quantity, bucket("mint"));

		associate(this.getAtomAddress(), tokenStateAddress.getHash());
		associate(this.getAtomAddress(), receiver);
		associate(this.getAtomAddress(), authority);
	}
	
	public void transfer(String symbol, UInt256 quantity, Identity sender, Identity receiver) throws ValidationException
	{
		symbol = Strings.toLowerCase(symbol);
		
		// TODO blind faithing it exists!
//		StateAddress tokenStateAddress = new StateAddress(context(), Hash.from(symbol));
//		assertExists(tokenStateAddress);
		
		vault(sender).take(symbol, quantity, bucket("transfer"));
		vault(receiver).put(symbol, quantity, bucket("transfer"));

		associate(this.getAtomAddress(), sender);
		associate(this.getAtomAddress(), receiver);
	}
}
