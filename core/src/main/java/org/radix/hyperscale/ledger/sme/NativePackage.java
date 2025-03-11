package org.radix.hyperscale.ledger.sme;

import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.StateContext;
import org.radix.hyperscale.utils.Strings;

final class NativePackage extends Package
{
	private final Class<? extends NativeComponent> componentClass;
	
	public NativePackage(final Class<? extends NativeComponent> componentClass)
	{
		super(StateAddress.from(Package.class, Strings.toLowerCase(componentClass.getAnnotation(StateContext.class).value())));
		
		this.componentClass = componentClass;
	}
	
	public Class<? extends NativeComponent> getComponentClass()
	{
		return this.componentClass;
	}

	@Override
	protected synchronized Hash computeHash() 
	{
		String context = Strings.toLowerCase(getClass().getAnnotation(StateContext.class).value());
		return Hash.valueOf(context);
	}
}
