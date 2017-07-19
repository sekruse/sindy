package de.hpi.isg.sindy.searchspace.cleaning;


import de.hpi.isg.sindy.util.IND;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Set;

/**
 * Cleans a negative border by testing all existing NINDs against the new NIND.
 */
public class NegativeBorderRetainingStrategy implements BorderCleaningStrategy {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void clean(IND newNind, Set<IND> border) {
        Iterator<IND> borderIterator = border.iterator();
        while (borderIterator.hasNext()) {
            IND borderNind = borderIterator.next();
            if (newNind.isImpliedBy(borderNind)) {
                this.logger.debug("Remove {} from negative border for {}.", borderNind, newNind);
                borderIterator.remove();
            }
        }
    }

}
