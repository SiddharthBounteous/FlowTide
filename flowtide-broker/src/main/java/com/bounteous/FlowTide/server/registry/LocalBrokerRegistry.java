package com.bounteous.FlowTide.server.registry;

/**
 * @deprecated Removed. Topic config is now owned exclusively by
 *             {@link com.bounteous.FlowTide.server.log.LogManager}.
 *             Broker self-tracking is no longer needed — the admin API
 *             queries flowtide-controller directly for cluster state.
 *
 *             This file is kept to avoid a hard delete; it is NOT a Spring bean
 *             and is not referenced anywhere in the codebase.
 */
@Deprecated
class LocalBrokerRegistry {
    // intentionally empty — see Javadoc above
}
