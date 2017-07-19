package de.hpi.isg.sodap.sindy.search_space.cleaning;


import de.hpi.isg.sindy.searchspace.cleaning.BorderCleaningStrategy;
import de.hpi.isg.sindy.util.IND;

import java.util.Set;

/**
 * This strategy does not attempt to do any cleaning. This is useful when adding dependencies that do not interact
 * with existing dependencies in the border.
 */
public class NoBorderCleaningStrategy implements BorderCleaningStrategy {

    private static NoBorderCleaningStrategy instance;

    public static NoBorderCleaningStrategy getInstance() {
        if (instance == null) {
            instance = new NoBorderCleaningStrategy();
        }
        return instance;
    }

    private NoBorderCleaningStrategy() {
        // Enforce singleton.
    }

    @Override
    public void clean(IND newNind, Set<IND> border) {
    }

}
