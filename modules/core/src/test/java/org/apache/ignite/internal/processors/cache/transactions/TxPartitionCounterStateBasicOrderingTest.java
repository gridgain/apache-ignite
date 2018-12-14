package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Print DHT transaction using tx callback framework.
 * TODO asserts on counted events.
 */
public class TxPartitionCounterStateBasicOrderingTest extends TxPartitionCounterStateAbstractTest {
    /** */
    public void testBasicTxCallback() throws Exception {
        int partId = 0;
        int backups = 2;
        int nodes = 3;
        int txSize = 5;

        runOnPartition(partId, backups, nodes, new TxCallback() {
            @Override public boolean beforePrimaryPrepare(IgniteEx node, IgniteUuid nearXidVer,
                GridFutureAdapter<?> proceedFut) {
                log.info("TX: beforePrimaryPrepare: prim=" + node.name() + ", nearXidVer=" + nearXidVer);

                return false;
            }

            @Override public boolean beforeBackupPrepare(IgniteEx prim, IgniteEx backup, IgniteInternalTx primaryTx,
                GridFutureAdapter<?> proceedFut) {

                log.info("TX: beforeBackupPrepare: prim=" + prim.name() + ", backup=" + backup.name() + ", nearXidVer=" + primaryTx.nearXidVersion().asGridUuid() + ", tx=" + CU.txString(primaryTx) );

                return false;
            }

            @Override public boolean beforePrimaryFinish(IgniteEx primaryNode, IgniteInternalTx primaryTx, GridFutureAdapter<?>
                proceedFut) {

                log.info("TX: beforePrimaryFinish: prim=" + primaryNode.name() + ", nearXidVer=" + primaryTx.nearXidVersion().asGridUuid() + ", tx=" + CU.txString(primaryTx));

                return false;
            }

            @Override public boolean afterPrimaryFinish(IgniteEx primaryNode, IgniteUuid nearXidVer, GridFutureAdapter<?> proceedFut) {
                log.info("TX: afterPrimaryFinish: prim=" + primaryNode.name() + ", nearXidVer=" + nearXidVer);

                return false;
            }

            @Override public boolean afterBackupPrepare(IgniteEx backup, IgniteInternalTx tx, GridFutureAdapter<?> fut) {
                log.info("TX: afterBackupPrepare: backup=" + backup.name() + ", backupTx=" + CU.txString(tx) + ", nearXidVer=" + tx.nearXidVersion().asGridUuid());

                return false;
            }

            @Override public boolean afterBackupFinish(IgniteEx backup, IgniteUuid nearXidVer, GridFutureAdapter<?> fut) {
                log.info("TX: afterBackupFinish: backup=" + backup.name() + ", nearXidVer=" + nearXidVer);

                return false;
            }

            @Override public boolean beforeBackupFinish(IgniteEx prim, IgniteEx backup,
                @Nullable IgniteInternalTx primTx,
                IgniteInternalTx backupTx,
                IgniteUuid nearXidVer, GridFutureAdapter<?> fut) {
                log.info("TX: beforeBackupFinish: prim=" + prim.name() + ", backup=" + backup.name() + ", primNearXidVer=" +
                    (primTx == null ? "NA" : primTx.nearXidVersion().asGridUuid()) + ", backupNearXidVer=" + backupTx.nearXidVersion().asGridUuid());

                return false;
            }

            @Override public boolean afterPrimaryPrepare(IgniteEx prim, IgniteInternalTx tx, GridFutureAdapter<?> fut) {
                log.info("TX: afterPrimaryPrepare: prim=" + prim.name() + ", nearXidVer=" + tx.nearXidVersion().asGridUuid() + ", tx=" + CU.txString(tx));

                return false;
            }

            @Override public void onTxStart(Transaction tx, int idx) {
            }
        }, new int[] {txSize});

        assertEquals(txSize, grid("client").cache(DEFAULT_CACHE_NAME).size());
    }
}
