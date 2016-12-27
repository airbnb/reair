package com.airbnb.reair.incremental;

/**
 * Class to encapsulate how the run of a replication task / job went.
 */
public class RunInfo {

  public enum RunStatus {
    // See similar definitions for
    // {@link com.airbnb.reair.incremental.ReplicationStatus}
    SUCCESSFUL,
    NOT_COMPLETABLE,
    FAILED,
    DEST_IS_NEWER,
  }

  private RunStatus runStatus;
  private long bytesCopied;

  public RunStatus getRunStatus() {
    return runStatus;
  }

  public void setRunStatus(RunStatus runStatus) {
    this.runStatus = runStatus;
  }

  public long getBytesCopied() {
    return bytesCopied;
  }

  public void setBytesCopied(long bytesCopied) {
    this.bytesCopied = bytesCopied;
  }

  public RunInfo(RunStatus runStatus, long bytesCopied) {
    this.runStatus = runStatus;
    this.bytesCopied = bytesCopied;
  }
}
