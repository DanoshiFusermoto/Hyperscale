package org.radix.hyperscale.database.vamos;

enum Operation 
{
	PUT(0), PUT_NO_OVERWRITE(1), DELETE(2), 
	
	EXT_NODE(64),

	START_TX(100), END_TX(101);
	
	static Operation get(int type)
	{
		switch(type)
		{
		case 0: return PUT;
		case 1: return PUT_NO_OVERWRITE;
		case 2: return DELETE;
			
		case 64: return EXT_NODE;

		case 100: return START_TX;
		case 101: return END_TX;
		
		default:
				throw new IllegalArgumentException("Operation type "+type+" is unknown");
		}
	}
	
	private final int type;
	
	Operation(int type)
	{
		this.type = type;
	}
	
	int type()
	{
		return this.type;
	}
}
