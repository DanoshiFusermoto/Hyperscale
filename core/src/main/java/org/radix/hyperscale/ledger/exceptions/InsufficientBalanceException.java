package org.radix.hyperscale.ledger.exceptions;

import java.util.Objects;

import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.exceptions.ValidationException;
import org.radix.hyperscale.utils.UInt256;

public class InsufficientBalanceException extends ValidationException
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 937989582224129068L;
	
	private final Identity spender;
	private final String symbol;
	private final UInt256 quantity;
	
	public InsufficientBalanceException(final Identity spender, final String symbol, final UInt256 quantity)
	{
		this("Insufficient balance to spend "+quantity+":"+symbol+" from "+spender, spender, symbol, quantity);
	}

	public InsufficientBalanceException(final String message, final Identity spender, final String symbol, final UInt256 quantity)
	{
		super(message);

		this.spender = Objects.requireNonNull(spender, "Spender is null");
		this.quantity = Objects.requireNonNull(quantity, "Quantity is null");
		this.symbol = Objects.requireNonNull(symbol, "Symbol is null");
	}

	public Identity getSpender() 
	{
		return this.spender;
	}

	public String getSymbol() 
	{
		return this.symbol;
	}

	public UInt256 getQuantity() 
	{
		return this.quantity;
	}
}
