package de.hpi.isg.sindy.mdms;

import com.beust.jcommander.Parameter;
import de.hpi.isg.sindy.searchspace.NaryIndRestrictions;

import java.io.Serializable;

/**
 * Parameters for the execution of the {@link SINDY} and its derivatives.
 *
 * @author Sebastian Kruse
 */
public class BasicParameters implements Serializable {

    private static final long serialVersionUID = 2936720486536771056L;

    @Parameter(names = {"--max-columns"}, description = "use only the first columns of each file", required = false)
    public int maxColumns = -1;

    @Parameter(names = {"--sample-rows"}, description = "how many percent of rows to take", required = false)
    public int sampleRows = -1;

    @Parameter(names = {"--no-nulls"}, description = "treat values/value combinations with null as non-existent", required = false)
    public boolean isDropNulls = false;

    @Parameter(names = {"--no-group-operators"},
            description = "use the old operators",
            required = false)
    public boolean isNotUseGroupOperators;

}
