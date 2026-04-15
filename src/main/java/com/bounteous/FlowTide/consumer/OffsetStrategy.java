package com.bounteous.FlowTide.consumer;

/** Determines where a consumer starts reading when no committed offset exists. */
public enum OffsetStrategy {
    /** Start from the very first event ever stored in the partition. */
    EARLIEST,
    /** Start from the next event to be written (skip all historical data). */
    LATEST,
    /** Resume from the last committed offset; falls back to EARLIEST if none exists. */
    COMMITTED
}
