package org.radix.hyperscale.executors;

public abstract class PollingProcessor extends Executable
{
	protected PollingProcessor()
	{
		super();
	}

	@Override
	public final void execute() 
	{
		try 
		{
			while(isTerminated() == false)
			{
                process();
			}
		}
		catch (Throwable throwable)
		{
			onError(throwable);
		}
		finally
		{
			onTerminated();
		}
	}
	
	public abstract void process() throws InterruptedException;
	
	public abstract void onError(Throwable thrown);

	public abstract void onTerminated();
}