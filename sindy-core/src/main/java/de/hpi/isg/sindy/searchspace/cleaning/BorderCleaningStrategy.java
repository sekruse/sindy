package de.hpi.isg.sindy.searchspace.cleaning;

import de.hpi.isg.sindy.util.IND;

import java.util.Set;

/**
 * Strategy to clean a positive or negative border when adding a new dependency.
 */
public interface BorderCleaningStrategy {

    /**
     * Remove any dependency from the border that is implied by the new dependency.
     *
     * @param newDependency is the dependency that should be added to the border (not added, yet, though)
     * @param border        is the border to that the dependency should be added and that should be cleaned
     */
    void clean(IND newDependency, Set<IND> border);

}
