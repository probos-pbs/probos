package uk.ac.gla.terrier.probos.master;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;

import com.cloudera.kitten.ContainerLaunchParameters;
import com.cloudera.kitten.appmaster.ApplicationMasterParameters;
import com.google.common.collect.Lists;

public class ProbosApplicationMasterDistributedServiceImpl extends
		ProbosApplicationMasterServiceImpl {

	private static final Logger LOG = LoggerFactory.getLogger(ProbosApplicationMasterDistributedServiceImpl.class);
	final List<NodeId> allocatedNodes = Lists.newArrayList();
	final AtomicBoolean broken = new AtomicBoolean(false);
	CountDownLatch superiorLatch;	
	Thread superiorThread = null;
	int requiredSisterCount;
	
	public ProbosApplicationMasterDistributedServiceImpl(
			ApplicationMasterParameters parameters, Configuration _conf)
			throws Exception {
		super(parameters, _conf);
		
		if (System.getenv("PBS_SISTER_COUNT") == null)
		{
			throw new IllegalArgumentException("Env invalid: PBS_SISTER_COUNT not found");
		}
		requiredSisterCount = Integer.parseInt(System.getenv("PBS_SISTER_COUNT"));
		LOG.info("Setting the superior task latch to "+ requiredSisterCount + " containers");
		superiorLatch = new CountDownLatch(requiredSisterCount);
	}
	
	@Override
	public void onContainersAllocated(List<Container> allocatedContainers) {
		for(Container c : allocatedContainers)
		{
			allocatedNodes.add(c.getNodeId());
		}
		super.onContainersAllocated(allocatedContainers);
	}

	@Override
	protected ContainerTracker getTracker(ContainerLaunchParameters clp) {
		if (clp.getEnvironment().containsKey("PBS_SISTER"))
			return new ProbosSisterContainerTracker(clp);
		return new ProbosSuperiorContainerTracker(clp);
	}
	
	protected class ProbosSuperiorContainerTracker extends ProbosContainerTracker {

		AtomicBoolean needsContainer = new AtomicBoolean();
		public ProbosSuperiorContainerTracker(ContainerLaunchParameters parameters) {
			super(parameters);
			needsContainer.set(true);
		}
		

		@Override
		public void launchContainer(final Container c) {
			if (broken.get())
			{
				LOG.error("ProbosSuperiorContainerTracker found the job was broken. Aborting superior start.");
				return;
			}
			
			needsContainer.set(false);
			//we need to return the AMRM servicing thread
			new Thread() {
				@Override
				public void run()
				{
					LOG.info("ProbosSuperiorContainerTracker is awaiting " + superiorLatch.getCount() + " sisters starting");
					try {
						final long starttime = System.currentTimeMillis();
						superiorThread = Thread.currentThread();
						superiorLatch.await();
						final long pausetime = System.currentTimeMillis();
						int controllerCount;						
						LOG.info("ProbosSuperiorContainerTracker paused "+ (pausetime - starttime) +"ms for all "
								+requiredSisterCount+" sisters to launch, now waiting for them to report to controller");
						while((controllerCount = masterClient.getDistributedHostCount(jobId)) != requiredSisterCount)
						{
							if (controllerCount < 0)
								throw new InterruptedException("Job "+jobId+" is unknown by controller!");
							Thread.sleep(500);
						}
						final long waittime = System.currentTimeMillis();
						LOG.info("ProbosSuperiorContainerTracker waited " + (waittime - pausetime) + "ms for all to report");
						Thread.sleep(4000);
					} catch (InterruptedException e) {
						LOG.error("ProbosSuperiorContainerTracker was interrupted waiting for sisters to start");
						return;
					}
					LOG.info("ProbosSuperiorContainerTracker now ready to start superior");
					ProbosSuperiorContainerTracker.super.ctxt.getEnvironment().put("PBS_NODELIST_EX", getNodeList());
					ProbosSuperiorContainerTracker.super.launchContainer(c);
				}
			}.start();			
		}
		
		@Override
		public void onContainerStopped(ContainerId containerId) {
			super.onContainerStopped(containerId);
			for (ContainerTracker tracker : trackers)
			{
				tracker.kill();
			}
		}


		@Override
		public void onStopContainerError(ContainerId containerId,
				Throwable throwable) {
			super.onStopContainerError(containerId, throwable);
			for (ContainerTracker tracker : trackers)
			{
				tracker.kill();
			}
		}
		
		@Override
		public boolean needsContainers() {
			if (! needsContainer.get())
				return false;
			return super.needsContainers();
		}

		protected String getNodeList()
		{
			StringBuilder s = new StringBuilder();
			for(NodeId n : allocatedNodes)
			{
				s.append(n.getHost());
				s.append(',');
			}
			s.setLength(s.length()-1);
			return s.toString();
		}
	}

	protected class ProbosSisterContainerTracker extends ContainerTracker {

		public ProbosSisterContainerTracker(ContainerLaunchParameters parameters) {
			super(parameters);
		}
		
		protected void abort()
		{
			broken.set(true);
			if (superiorThread != null)
				superiorThread.interrupt();
		}
		
		@Override
		public void launchContainer(Container c) {
			super.launchContainer(c);
			allocatedNodes.add(c.getNodeId());
			LOG.info("SisterContainer "+ c.getId().toString() + " launched on " + c.getNodeId());
		}

		@Override
		public void kill() {
			super.kill();
			abort();
		}
		
		

		@Override
		public void onContainerStarted(ContainerId containerId,
				Map<String, ByteBuffer> allServiceResponse) {
			super.onContainerStarted(containerId, allServiceResponse);
			superiorLatch.countDown();
			LOG.info("SisterContainer "+containerId.toString() + ", " + superiorLatch.getCount() + " sisters remaining");
		}

		@Override
		public void onStartContainerError(ContainerId containerId,
				Throwable throwable) {
			super.onStartContainerError(containerId, throwable);
			abort();
		}

		@Override
		public void onStopContainerError(ContainerId containerId,
				Throwable throwable) {
			super.onStopContainerError(containerId, throwable);
			abort();
		}
	}

}
