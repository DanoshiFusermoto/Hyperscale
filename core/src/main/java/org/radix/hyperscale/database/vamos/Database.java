package org.radix.hyperscale.database.vamos;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Objects;

import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

public class Database 
{
	private static final Logger vamosLog = Logging.getLogger("vamos");

    static final int	 	defaultBlockSize = 1<<12;	// default bloom block size
    static final Charset 	charset = Charset.forName("UTF-8"); // encoding used for storing hash values as strings
    
    private final Environment environment;
    
    private final int id;
    private final String name;
    private final DatabaseConfig config;
    
    private long totalOperations = 0;
    private long totalRecords = 0;

    private long opened = System.currentTimeMillis();

	Database(Environment environment, String name, DatabaseConfig config) throws IOException 
	{
		this.environment = Objects.requireNonNull(environment, "Environment is null");
		this.config = Objects.requireNonNull(config, "Config is null");
		this.name = Objects.requireNonNull(name, "Name is null").toLowerCase();
		this.id = this.name.hashCode(); // TODO better ID calculation (not java hashcode dependent)
	}

	public void close() throws IOException
	{
	}
	
	Environment getEnvironment()
	{
		return this.environment;
	}

	DatabaseConfig getConfig()
	{
		return this.config;
	}
	
	public int getID()
	{
		return this.id;
	}

	public String getName()
	{
		return this.name;
	}

	public OperationStatus delete(Transaction transaction, DatabaseEntry key, LockMode lockMode) throws IOException 
    {
    	if (transaction == null)
    	{
    		OperationStatus status;
    		transaction = this.environment.getTransactionPool().acquire();
    		try
    		{
    			status = deleteInternal(transaction, key, lockMode);
    			
    			transaction.commit();
    		}
    		catch (Exception ex)
    		{
    			transaction.abort();
    			throw ex;
    		}
    		finally
    		{
    			this.environment.getTransactionPool().release(transaction);
    		}
    		
    		return status;
    	}
    	else
    		return deleteInternal(transaction, key, lockMode);
    }
    
    private OperationStatus deleteInternal(Transaction transaction, DatabaseEntry key, LockMode lockMode) throws IOException
    {
    	InternalKey internalKey = InternalKey.from(this, key);
		IndexNodeID indexNodeID = this.environment.toIndexNodeID(internalKey);

		try 
		{
			OperationStatus status = transaction.delete(this, key, internalKey, indexNodeID, lockMode);
			return status;
		} 
    	catch (IOException ioex)
    	{
    		vamosLog.error(ioex);
    		throw ioex;
    	}
        finally
        {
        	this.totalOperations++;
    		logInfo();
        }
    }

    public OperationStatus put(Transaction transaction, DatabaseEntry key, DatabaseEntry value, LockMode lockMode) throws IOException 
    {
    	return put(transaction, key, value, Operation.PUT, lockMode);
    }

    public OperationStatus putNoOverwrite(Transaction transaction, DatabaseEntry key, DatabaseEntry value, LockMode lockMode) throws IOException 
    {
    	return put(transaction, key, value, Operation.PUT_NO_OVERWRITE, lockMode);
    }
    
    public OperationStatus put(Transaction transaction, DatabaseEntry key, DatabaseEntry value, Operation operation, LockMode lockMode) throws IOException 
    {
    	if (transaction == null)
    	{
    		OperationStatus status;
    		transaction = this.environment.getTransactionPool().acquire();
    		try
    		{
    			status = putInternal(transaction, key, value, operation, lockMode);
    			
    			transaction.commit();
    		}
    		catch (Exception ex)
    		{
    			transaction.abort();
    			throw ex;
    		}
    		finally
    		{
    			this.environment.getTransactionPool().release(transaction);
    		}

    		return status;
    	}
    	else
    		return putInternal(transaction, key, value, operation, lockMode);
    }
    
    private OperationStatus putInternal(Transaction transaction, DatabaseEntry key, DatabaseEntry value, Operation operation, LockMode lockMode) throws IOException
    {
    	if (operation.equals(Operation.PUT) == false && operation.equals(Operation.PUT_NO_OVERWRITE) == false)
    		throw new IllegalArgumentException("Operation type is not a PUT");
    	
    	final InternalKey internalKey = InternalKey.from(this, key);
    	final IndexNodeID indexNodeID = this.environment.toIndexNodeID(internalKey);

		try 
		{
			OperationStatus status = transaction.put(this, key, value, internalKey, indexNodeID, operation, lockMode);
			return status;
		} 
    	catch (IOException ioex)
    	{
    		vamosLog.error(ioex);
    		throw ioex;
    	}
        finally
        {
        	this.totalOperations++;
    		logInfo();
        }
    }
    
	public OperationStatus get(Transaction transaction, DatabaseEntry key, DatabaseEntry value, LockMode lockMode) throws IOException
	{
		if (transaction == null)
		{
			OperationStatus status;
    		transaction = this.environment.getTransactionPool().acquire();
			try
			{
				status = getInternal(transaction, key, value, lockMode);
				
				transaction.commit();
			}
			catch (Exception ex)
			{
				transaction.abort();
				throw ex;
			}
    		finally
    		{
    			this.environment.getTransactionPool().release(transaction);
    		}

			return status;
		}
		else
			return getInternal(transaction, key, value, lockMode);
	}

	private OperationStatus getInternal(Transaction transaction, DatabaseEntry key, DatabaseEntry value, LockMode lockMode) throws IOException 
    {
    	InternalKey internalKey = InternalKey.from(this, key);
		IndexNodeID indexNodeID = this.environment.toIndexNodeID(internalKey);
		
		try 
		{
			OperationStatus status = transaction.get(this, key, value, internalKey, indexNodeID, lockMode);
			return status;
		} 
		catch (IOException ioex)
    	{
    		vamosLog.error(ioex);
    		throw ioex;
    	}
        finally
        {
        	this.totalOperations++;
    		logInfo();
        }
    }
	
	boolean exists(Transaction transaction, InternalKey internalKey) throws IOException
	{
		final IndexItem indexItem = this.environment.getIndex().getIndexItem(transaction, internalKey);
		Objects.requireNonNull(indexItem, "Index item is null");
		return indexItem == IndexItem.VACANT ? false : true;
	}
        
    private void logInfo()
    {
		if (this.totalOperations> 0 && this.totalOperations % 100000 == 0)
		{
			String info = this.name+": Records: "+this.totalRecords+" Operations: "+this.totalOperations+"/"+(this.totalOperations / Math.max(1, ((System.currentTimeMillis() - opened) / 1000)));
//			info += " Idx Node R/W/CR/CW: "+this.indexNodeReads+"/"+this.indexNodeWrites+"/"+this.contiguousIndexNodeReads+"/"+this.contiguousIndexNodeWrites;
//			info += " Idx R/W/CR/CW: "+this.indexPositionReads+"/"+this.indexPositionWrites+"/"+this.contiguousIndexPositionReads+"/"+this.contiguousIndexPositionWrites;
//			info += " Log R/W/CR/CW: "+this.logReads+"/"+this.logWrites+"/"+this.contiguousLogReads+"/"+this.contiguousLogWrites;
//			if (this.config.getAllowDuplicates() == true)
//				info += " Ext Node R/W/CR/CW: "+this.extensionNodeReads+"/"+this.extensionNodeWrites+"/"+this.contiguousExtensionReads+"/"+this.contiguousExtensionWrites+" Ext Item R/W: "+this.extensionItemReads+"/"+this.extensionItemWrites;
			
			vamosLog.info(info);
		}
    }
}
