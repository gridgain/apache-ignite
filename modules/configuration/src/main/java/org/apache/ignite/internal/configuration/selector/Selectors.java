package org.apache.ignite.internal.configuration.selector;

import org.apache.ignite.internal.configuration.Keys;
import org.apache.ignite.internal.configuration.getpojo.AutoAdjust;
import org.apache.ignite.internal.configuration.getpojo.Baseline;
import org.apache.ignite.internal.configuration.getpojo.Local;
import org.apache.ignite.internal.configuration.getpojo.Node;
import org.apache.ignite.internal.configuration.initpojo.InitAutoAdjust;
import org.apache.ignite.internal.configuration.initpojo.InitBaseline;
import org.apache.ignite.internal.configuration.initpojo.InitLocal;
import org.apache.ignite.internal.configuration.initpojo.InitNode;
import org.apache.ignite.internal.configuration.internalconfig.AutoAdjustConfiguration;
import org.apache.ignite.internal.configuration.internalconfig.BaselineConfiguration;
import org.apache.ignite.internal.configuration.internalconfig.DynamicProperty;
import org.apache.ignite.internal.configuration.internalconfig.LocalConfiguration;
import org.apache.ignite.internal.configuration.internalconfig.NodeConfiguration;
import org.apache.ignite.internal.configuration.setpojo.ChangeAutoAdjust;
import org.apache.ignite.internal.configuration.setpojo.ChangeBaseline;
import org.apache.ignite.internal.configuration.setpojo.ChangeLocal;
import org.apache.ignite.internal.configuration.setpojo.ChangeNode;
import org.apache.ignite.internal.configuration.setpojo.NList;

/**
 * TODO: Add interface description.
 *
 * @author @java.author
 * @version @java.version
 */
public class Selectors {
    public static final Selector<Local, ChangeLocal, InitLocal, LocalConfiguration> LOCAL = new Selector<>(Keys.LOCAL);
    public static final Selector<Baseline, ChangeBaseline, InitBaseline, BaselineConfiguration> LOCAL_BASELINE = new Selector<>(Keys.LOCAL_BASELINE);
    public static final Selector<AutoAdjust, ChangeAutoAdjust, InitAutoAdjust, AutoAdjustConfiguration> LOCAL_BASELINE_AUTO_ADJUST = new Selector<>(Keys.LOCAL_BASELINE_AUTO_ADJUST);

    public static final Selector<Node, NList<ChangeNode>, NList<InitNode>, NodeConfiguration> LOCAL_BASELINE_NODES = new NodesSelector();

    public static final Selector<Long, Long, Long, DynamicProperty<Long>> LOCAL_BASELINE_AUTO_ADJUST_TIMEOUT = new Selector<>(Keys.LOCAL_BASELINE_AUTO_ADJUST_TIMEOUT);
    public static final Selector<Boolean, Boolean, Boolean, DynamicProperty<Boolean>> LOCAL_BASELINE_AUTO_ADJUST_ENABLED = new Selector<>(Keys.LOCAL_BASELINE_AUTO_ADJUST_ENABLED);

    public static NodeSelector localBaselineNode(String name) {
        return new NodeSelector(name);
    }
}
