package de.hpi.isg.sindy.searchspace.cleaning;


import de.hpi.isg.sindy.util.IND;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Set;

/**
 * Cleans a positive border by testing all existing INDs against the new IND.
 */
public class PositiveBorderRetainingStrategy implements BorderCleaningStrategy {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void clean(IND newInd, Set<IND> border) {
        Iterator<IND> borderIterator = border.iterator();
        while (borderIterator.hasNext()) {
            IND borderInd = borderIterator.next();
            if (borderInd.isImpliedBy(newInd)) {
                this.logger.debug("Remove {} from positive border for {}.", borderInd, newInd);
                borderIterator.remove();
            }
        }
    }

}
