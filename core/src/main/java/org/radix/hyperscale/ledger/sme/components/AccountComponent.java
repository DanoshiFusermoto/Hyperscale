package org.radix.hyperscale.ledger.sme.components;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.StateContext;
import org.radix.hyperscale.ledger.StateLockMode;
import org.radix.hyperscale.ledger.sme.Component;
import org.radix.hyperscale.ledger.sme.NativeComponent;
import org.radix.hyperscale.ledger.sme.StateMachine;

@StateContext("account")
public final class AccountComponent extends NativeComponent 
{
	static
	{
		Component.registerNative(AccountComponent.class);
	}
	
	public AccountComponent(StateMachine stateMachine)
	{
		super(stateMachine, AccountComponent.class);
	}
	
	public void create_prep(Identity identity) 
	{
		prep(identity);
	}
	
	public void set_jurisdiction_prep(Identity identity, String jurisdiction) 
	{
		prep(identity);
	}

	public void jurisdictions_accept_add_prep(Identity identity, List<String> tokens) 
	{
		prep(identity);
	}

	public void jurisdictions_accept_set_prep(Identity identity, List<String> tokens) 
	{
		prep(identity);
	}

	public void jurisdictions_reject_add_prep(Identity identity, List<String> tokens) 
	{
		prep(identity);
	}

	public void jurisdictions_reject_set_prep(Identity identity, List<String> tokens) 
	{
		prep(identity);
	}

	public void tokens_accept_add_prep(Identity identity, List<String> tokens) 
	{
		prep(identity);
	}

	public void tokens_accept_set_prep(Identity identity, List<String> tokens) 
	{
		prep(identity);
	}

	public void tokens_reject_add_prep(Identity identity, List<String> tokens) 
	{
		prep(identity);
	}

	public void tokens_reject_set_prep(Identity identity, List<String> tokens) 
	{
		prep(identity);
	}

	public void identities_accept_add_prep(Identity identity, List<Identity> identities) 
	{
		prep(identity);
	}

	public void identities_accept_set_prep(Identity identity, List<Identity> identities) 
	{
		prep(identity);
	}

	public void identities_reject_add_prep(Identity identity, List<Identity> identities) 
	{
		prep(identity);
	}

	public void identities_reject_set_prep(Identity identity, List<Identity> identities) 
	{
		prep(identity);
	}

	private void prep(Identity identity)
	{
		lock(StateAddress.from(context(), identity), StateLockMode.WRITE);
	}
	
	public void create(Identity identity)
	{
		StateAddress accountStateAddress = StateAddress.from(context(), identity);
		assertCreate(accountStateAddress, identity);
			
		set(accountStateAddress, "tokens.accept", Collections.singletonList("*"));
		set(accountStateAddress, "jurisdiction.accept", Collections.singletonList("*"));
		set(accountStateAddress, "identities.accept", Collections.singletonList(Identity.NULL));
		associate(accountStateAddress, identity);
	}
	
	public void set_jurisdiction(Identity identity, String jurisdiction)
	{
		StateAddress accountStateAddress = StateAddress.from(context(), identity);
		assertExists(accountStateAddress);
		
		set(accountStateAddress, "jurisdiction", jurisdiction);
	}

	// TOKEN WHITELISTING
	public void tokens_accept_add(Identity identity, List<String> tokens)
	{
		StateAddress accountStateAddress = StateAddress.from(context(), identity);
		assertExists(accountStateAddress);
		
		Set<String> acceptedTokens = new LinkedHashSet<String>(get(accountStateAddress, "tokens.accept"));
		acceptedTokens.addAll(tokens);
		set(accountStateAddress, "tokens.accept", acceptedTokens);
	}
	
	public void tokens_accept_set(Identity identity, List<String> tokens)
	{
		StateAddress accountStateAddress = StateAddress.from(context(), identity);
		assertExists(accountStateAddress);
		
		Set<String> acceptedTokens;
		if (tokens.isEmpty())
			acceptedTokens = Collections.singleton("*");
		else
			acceptedTokens = new LinkedHashSet<String>(tokens);
		
		set(accountStateAddress, "tokens.accept", acceptedTokens);
	}

	public void tokens_reject_add(Identity identity, List<String> tokens)
	{
		StateAddress accountStateAddress = StateAddress.from(context(), identity);
		assertExists(accountStateAddress);
		
		Set<String> rejectedTokens = new LinkedHashSet<String>(get(accountStateAddress, "tokens.reject", Collections.emptyList()));
		rejectedTokens.addAll(tokens);
		set(accountStateAddress, "tokens.reject", rejectedTokens);
	}
	
	public void tokens_reject_set(Identity identity, List<String> tokens)
	{
		StateAddress accountStateAddress = StateAddress.from(context(), identity);
		assertExists(accountStateAddress);
		
		Set<String> rejectedTokens;
		if (tokens.isEmpty())
			rejectedTokens = Collections.emptySet();
		else
			rejectedTokens = new LinkedHashSet<String>(tokens);
		
		set(accountStateAddress, "tokens.reject", rejectedTokens);
	}

	// JURISDICTION WHITELISTING
	public void jurisditions_accept_add(Identity identity, List<String> jurisditions)
	{
		StateAddress accountStateAddress = StateAddress.from(context(), identity);
		assertExists(accountStateAddress);
		
		Set<String> acceptedJurisdictions = new LinkedHashSet<String>(get(accountStateAddress, "jurisditions.accept", Collections.emptyList()));
		acceptedJurisdictions.addAll(jurisditions);
		set(accountStateAddress, "jurisditions.accept", acceptedJurisdictions);
	}
	
	public void jurisditions_accept_set(Identity identity, List<String> jurisditions)
	{
		StateAddress accountStateAddress = StateAddress.from(context(), identity);
		assertExists(accountStateAddress);
		
		Set<String> acceptedJurisdictions;
		if (jurisditions.isEmpty())
			acceptedJurisdictions = Collections.singleton("*");
		else
			acceptedJurisdictions = new LinkedHashSet<String>(jurisditions);
		
		set(accountStateAddress, "jurisditions.accept", acceptedJurisdictions);
	}

	public void jurisditions_reject_add(Identity identity, List<String> jurisditions)
	{
		StateAddress accountStateAddress = StateAddress.from(context(), identity);
		assertExists(accountStateAddress);
		
		Set<String> rejectedJurisdictions = new LinkedHashSet<String>(get(accountStateAddress, "jurisditions.reject", Collections.emptyList()));
		rejectedJurisdictions.addAll(jurisditions);
		set(accountStateAddress, "jurisditions.reject", rejectedJurisdictions);
	}
	
	public void jurisditions_reject_set(Identity identity, List<String> jurisditions)
	{
		StateAddress accountStateAddress = StateAddress.from(context(), identity);
		assertExists(accountStateAddress);
		
		Set<String> rejectedJurisdictions;
		if (jurisditions.isEmpty())
			rejectedJurisdictions = Collections.emptySet();
		else
			rejectedJurisdictions = new LinkedHashSet<String>(jurisditions);
		
		set(accountStateAddress, "jurisditions.reject", rejectedJurisdictions);
	}

	// IDENTITIES WHITELISTING
	public void identities_accept_add(Identity identity, List<Identity> identities)
	{
		StateAddress accountStateAddress = StateAddress.from(context(), identity);
		assertExists(accountStateAddress);
		
		Set<Identity> acceptedIdentities = new LinkedHashSet<Identity>(get(accountStateAddress, "identities.accept", Collections.emptyList()));
		acceptedIdentities.addAll(identities);
		set(accountStateAddress, "identities.accept", acceptedIdentities);
	}
	
	public void identities_accept_set(Identity identity, List<Identity> identities)
	{
		StateAddress accountStateAddress = StateAddress.from(context(), identity);
		assertExists(accountStateAddress);
		
		Set<Identity> acceptedIdentities;
		if (identities.isEmpty())
			acceptedIdentities = Collections.singleton(Identity.NULL);
		else
			acceptedIdentities = new LinkedHashSet<Identity>(identities);
		
		set(accountStateAddress, "identities.accept", acceptedIdentities);
	}

	public void identities_reject_add(Identity identity, List<Identity> identities)
	{
		StateAddress accountStateAddress = StateAddress.from(context(), identity);
		assertExists(accountStateAddress);
		
		Set<Identity> rejectedIdentities = new LinkedHashSet<Identity>(get(accountStateAddress, "identities.reject", Collections.emptyList()));
		rejectedIdentities.addAll(identities);
		set(accountStateAddress, "identities.reject", rejectedIdentities);
	}
	
	public void identities_reject_set(Identity identity, List<Identity> identities)
	{
		StateAddress accountStateAddress = StateAddress.from(context(), identity);
		assertExists(accountStateAddress);
		
		Set<Identity> rejectedIdentities;
		if (identities.isEmpty())
			rejectedIdentities = Collections.emptySet();
		else
			rejectedIdentities = new LinkedHashSet<Identity>(identities);
		
		set(accountStateAddress, "identities.reject", rejectedIdentities);
	}
}