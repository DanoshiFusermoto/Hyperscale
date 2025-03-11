package org.radix.hyperscale.database.vamos;

/**
 * <p>Specifies the attributes of a database.</p>
 */
public class DatabaseConfig implements Cloneable 
{
    /**
     * An instance created using the default constructor is initialized with
     * the system's default settings.
     */
    public static final DatabaseConfig DEFAULT = new DatabaseConfig();

    private boolean allowCreate = false;
    private boolean exclusiveCreate = false;
    private boolean readOnly = false;
    private boolean allowDuplicates = false;

    /**
     * An instance created using the default constructor is initialized with
     * the system's default settings.
     */
    public DatabaseConfig() 
    {
    }
    
    public DatabaseConfig setAllowCreate(boolean allowCreate) 
    {
        this.allowCreate = allowCreate;
        return this;
    }

    public boolean getAllowCreate() 
    {
        return this.allowCreate;
    }

    public DatabaseConfig setExclusiveCreate(boolean exclusiveCreate) 
    {
        this.exclusiveCreate = exclusiveCreate;
        return this;
    }

    public boolean getExclusiveCreate() 
    {
        return this.exclusiveCreate;
    }

    public DatabaseConfig setAllowDuplicates(boolean allowDuplicates)
    {
        this.allowDuplicates = allowDuplicates;
        return this;
    }

    public boolean getAllowDuplicates()
    {
        return this.allowDuplicates;
    }

    public DatabaseConfig setReadOnly(boolean readOnly) 
    {
        this.readOnly = readOnly;
        return this;
    }

    public boolean getReadOnly() 
    {
        return this.readOnly;
    }
}
