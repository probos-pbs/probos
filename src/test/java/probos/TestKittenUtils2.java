/**
 * Copyright (c) 2016, University of Glasgow. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package probos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import gnu.trove.set.hash.TIntHashSet;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Test;

import uk.ac.gla.terrier.probos.Constants;
import uk.ac.gla.terrier.probos.JobUtils;
import uk.ac.gla.terrier.probos.PConfiguration;
import uk.ac.gla.terrier.probos.Utils;
import uk.ac.gla.terrier.probos.Version;
import uk.ac.gla.terrier.probos.api.PBSJob;
import uk.ac.gla.terrier.probos.controller.KittenUtils2;

import com.cloudera.kitten.ContainerLaunchParameters;
import com.cloudera.kitten.appmaster.params.lua.LuaApplicationMasterParameters;
import com.cloudera.kitten.client.params.lua.LuaYarnClientParameters;
import com.google.common.collect.ImmutableMap;

public class TestKittenUtils2 {

	private Map<String, Object> extraLuaValues = ImmutableMap.<String, Object>of();
	private Map<String, String> extraLocalResources = ImmutableMap.<String, String>of();
	
	protected LuaYarnClientParameters testJobCompilesClient(PBSJob job) throws Exception
	{
		int newId = 1;
		File luaFile = File.createTempFile("job" + String.valueOf(newId), ".lua");
		KittenUtils2 ku = KittenUtils2.createKittenUtil(new PConfiguration(), job, newId);
		ku.writeKJobToFile(new Path(job.getCommand()), luaFile);
		return new LuaYarnClientParameters(
			luaFile.toString(), Constants.PRODUCT_NAME, new Configuration(), extraLuaValues, extraLocalResources);
	}
	
	protected LuaApplicationMasterParameters testJobCompilesMaster(PBSJob job) throws Exception
	{
		int newId = 1;
		File luaFile = File.createTempFile("job" + String.valueOf(newId), ".lua");
		KittenUtils2 ku = KittenUtils2.createKittenUtil(new PConfiguration(), job, newId);
		ku.writeKJobToFile(new Path(job.getCommand()), luaFile);
		return new LuaApplicationMasterParameters(
			luaFile.toString(), Constants.PRODUCT_NAME, new Configuration());
	}
	
	@Test
	public void testHostnameMatching()
	{
		assertTrue(KittenUtils2.isNode("trnode01"));
		assertTrue(KittenUtils2.isNode("trnode-01"));
		assertTrue(KittenUtils2.isNode("node01"));		
		assertTrue(KittenUtils2.isNode("node-01"));
		assertFalse(KittenUtils2.isNode("salt"));
		assertFalse(KittenUtils2.isNode("master"));
		
		
	}
	
	@Test
	public void testLuaSimpleJob() throws Exception {
		String jobName = "testHostname";
		assertTrue(new File("target/probos-"+Version.VERSION+".jar").exists());
		assertTrue(new File("lib/kitten-master-"+Version.KITTEN_VERSION+"-jar-with-dependencies.jar").exists());
		PBSJob job = UtilsForTest.getSimpleJob(jobName, "hostname");
		
		//test the client aspect
		LuaYarnClientParameters c1 = testJobCompilesClient(job);
		ContainerLaunchParameters clpM = c1.getApplicationMasterParameters(ApplicationId.newInstance(System.currentTimeMillis(), 1));
		assertTrue(clpM.getEnvironment().containsKey("CLASSPATH"));
		assertTrue(clpM.getEnvironment().containsKey("PBS_CONTROLLER"));
		assertTrue(clpM.getEnvironment().containsKey("PBS_JOBID"));
		
		
		//test the master aspect
		LuaApplicationMasterParameters c2 = testJobCompilesMaster(job);
		List<ContainerLaunchParameters> clpI = c2.getContainerLaunchParameters();
		for(ContainerLaunchParameters clp : clpI)
		{
			for(String mapped : new String[]{"PATH", "HOME", "TZ", "LANG", "MAIL", "SHELL"})
			{
				if (System.getenv(mapped) != null)
					assertEquals("Bad job env for " + mapped,  System.getenv(mapped), clp.getEnvironment().get("PBS_O_" + mapped));
			}	
			assertEquals("1", clp.getEnvironment().get("PBS_JOBID"));
			assertEquals(JobUtils.DEFAULT_QUEUE,  clp.getEnvironment().get("PBS_O_QUEUE"));
			assertEquals(Utils.getHostname(), clp.getEnvironment().get("PBS_O_HOST"));
			assertEquals("PBS_BATCH", clp.getEnvironment().get("PBS_ENVIRONMENT"));
			assertEquals("1", clp.getEnvironment().get("PBS_CORES"));
			assertEquals("512", clp.getEnvironment().get("PBS_VMEM"));
			assertEquals("$(pwd)/tmp", clp.getEnvironment().get("TMP"));
			
			//now check that resources are generated properly
			Map<String, LocalResource> resources = clp.getLocalResources();
			assertNotNull(resources);
			assertTrue(resources.containsKey("job.SC"));
			LocalResource r = resources.get("job.SC");
			assertNotNull(r);
//			System.err.println(r.getResource());
//			System.err.println(resources);
		}
	}
	
	@Test
	public void testLuaJobComplexName() throws Exception {
		PBSJob job = UtilsForTest.getSimpleJob("yarn-2", "hostname");
		testJobCompilesClient(job);
	}
	
	@Test
	public void testLuaInteractive() throws Exception {
		PBSJob job;
		job = UtilsForTest.getSimpleJob("testLuaInteractive", "#PBS -I");
		testJobCompilesClient(job);
	}
	
	//@Test
	public void testLuaGPUs() throws Exception {
		PBSJob job;
		int gpus = 2;
		job = UtilsForTest.getSimpleJob("testLuaGPU", "#PBS -l nodes=1:gpus=" + gpus);
		
		LuaApplicationMasterParameters lamp = testJobCompilesMaster(job);
		List<ContainerLaunchParameters> clpI = lamp.getContainerLaunchParameters();
		assertEquals(1, clpI.size());
		ContainerLaunchParameters clpTask = clpI.get(0);
		//THIS doesn't work, as we need a resource-type setup with GPUs
		Resource MAX = Resource.newInstance(Integer.MAX_VALUE, 100, ImmutableMap.of(ResourceInformation.GPU_URI, 3l));
		Resource r;
//		MAX.setMemorySize(Integer.MAX_VALUE);
//		MAX.setVirtualCores(100);
//		MAX.setResourceValue(ResourceInformation.GPU_URI, 3);
		r = clpTask.getContainerResource(MAX);
		assertEquals(gpus, r.getResourceInformation(ResourceInformation.GPU_URI));
		assertEquals(1, r.getVirtualCores());
		
		MAX.setMemorySize(8192);
		MAX.setVirtualCores(100);
		r = clpTask.getContainerResource(MAX);
		assertEquals(8192, r.getMemorySize());
		assertEquals(1, r.getVirtualCores());
		
	}
	
	@Test
	public void testLuaMemory() throws Exception {
		PBSJob job;
		int mem = 32000;
		job = UtilsForTest.getSimpleJob("testLuaMemory", "#PBS -l nodes=1:mem=" + mem);
		
		LuaApplicationMasterParameters lamp = testJobCompilesMaster(job);
		List<ContainerLaunchParameters> clpI = lamp.getContainerLaunchParameters();
		assertEquals(1, clpI.size());
		ContainerLaunchParameters clpTask = clpI.get(0);
		Resource MAX = Records.newRecord(Resource.class);
		Resource r;
		MAX.setMemorySize(Integer.MAX_VALUE);
		MAX.setVirtualCores(100);
		r = clpTask.getContainerResource(MAX);
		assertEquals(mem, r.getMemorySize());
		assertEquals(1, r.getVirtualCores());
		
		MAX.setMemorySize(8192);
		MAX.setVirtualCores(100);
		r = clpTask.getContainerResource(MAX);
		assertEquals(8192, r.getMemorySize());
		assertEquals(1, r.getVirtualCores());
		
	}
	
	@Test
	public void testLuaMasterDefaults() throws Exception {
		PBSJob job;
		job = UtilsForTest.getSimpleJob("testLuaMaster", "export", new String[]{});
		
		Resource MAX = Records.newRecord(Resource.class);
		MAX.setMemorySize(Integer.MAX_VALUE);
		MAX.setVirtualCores(100);
		
		LuaYarnClientParameters lamp = testJobCompilesClient(job);
		assertEquals(512, lamp.getApplicationMasterParameters(null).getContainerResource(MAX).getMemorySize());
		assertEquals(1, lamp.getApplicationMasterParameters(null).getContainerResource(MAX).getVirtualCores());
	}
	
	@Test
	public void testLuaMasterMemoryCmdline() throws Exception {
		PBSJob job;
		int mem = 32000;
		job = UtilsForTest.getSimpleJob("testLuaMasterMemory", "export", new String[]{"-l", "master=1:mem=" + mem});
		
		Resource MAX = Records.newRecord(Resource.class);
		MAX.setMemorySize(Integer.MAX_VALUE);
		MAX.setVirtualCores(100);
		
		LuaYarnClientParameters lamp = testJobCompilesClient(job);
		assertEquals(mem, lamp.getApplicationMasterParameters(null).getContainerResource(MAX).getMemorySize());
		//default is 1 core when master is specified
		assertEquals(1, lamp.getApplicationMasterParameters(null).getContainerResource(MAX).getVirtualCores());
	}
	
	@Test
	public void testLuaMasterMemoryCores() throws Exception {
		PBSJob job;
		int cores = 3;
		job = UtilsForTest.getSimpleJob("testLuaMasterMemory", "export", new String[]{"-l", "master=1:ppn=" + 3});
		
		Resource MAX = Records.newRecord(Resource.class);
		MAX.setMemorySize(Integer.MAX_VALUE);
		MAX.setVirtualCores(100);
		
		LuaYarnClientParameters lamp = testJobCompilesClient(job);
		assertEquals(512, lamp.getApplicationMasterParameters(null).getContainerResource(MAX).getMemorySize());
		assertEquals(cores, lamp.getApplicationMasterParameters(null).getContainerResource(MAX).getVirtualCores());
	}
	
	@Test
	public void testLuaMemoryCmdline() throws Exception {
		PBSJob job;
		int mem = 32000;
		job = UtilsForTest.getSimpleJob("testLuaMemory", "export", new String[]{"-l", "nodes=1:mem=" + mem});
		
		LuaApplicationMasterParameters lamp = testJobCompilesMaster(job);
		List<ContainerLaunchParameters> clpI = lamp.getContainerLaunchParameters();
		assertEquals(1, clpI.size());
		ContainerLaunchParameters clpTask = clpI.get(0);
		Resource MAX = Records.newRecord(Resource.class);
		Resource r;
		MAX.setMemorySize(Integer.MAX_VALUE);
		MAX.setVirtualCores(100);
		r = clpTask.getContainerResource(MAX);
		assertEquals(mem, r.getMemorySize());
		assertEquals(1, r.getVirtualCores());
		
		MAX.setMemorySize(8192);
		MAX.setVirtualCores(100);
		r = clpTask.getContainerResource(MAX);
		assertEquals(8192, r.getMemorySize());
		assertEquals(1, r.getVirtualCores());
		
	}
	
	@Test
	public void testLuaMemoryCmdlineSuffix() throws Exception {
		PBSJob job;
		int mem = 32;
		job = UtilsForTest.getSimpleJob("testLuaMemory", "export", new String[]{"-l", "nodes=1:mem=" + mem + "g"});
		
		LuaApplicationMasterParameters lamp = testJobCompilesMaster(job);
		List<ContainerLaunchParameters> clpI = lamp.getContainerLaunchParameters();
		assertEquals(1, clpI.size());
		ContainerLaunchParameters clpTask = clpI.get(0);
		Resource MAX = Records.newRecord(Resource.class);
		Resource r;
		MAX.setMemorySize(Integer.MAX_VALUE);
		MAX.setVirtualCores(100);
		r = clpTask.getContainerResource(MAX);
		assertEquals(mem * 1024, r.getMemorySize());
		assertEquals(1, r.getVirtualCores());
		
		MAX.setMemorySize(8192);
		MAX.setVirtualCores(100);
		r = clpTask.getContainerResource(MAX);
		assertEquals(8192, r.getMemorySize());
		assertEquals(1, r.getVirtualCores());
		
	}
	

	@Test
	public void testLuaDistributed() throws Exception {
		PBSJob job;
		int i=0;
		String[] opts = new String[]{
			"nodes=1",//0
			"nodes=trnode11",//1
			"nodes=trnode11:ppn=3",	//2		
			"nodes=1:ppn=3",//3
			"nodes=1:ppn=3,mem=100",//4
			"nodes=2:ppn=3,mem=100",//5
			"nodes=2:blue:ppn=2", //6
			"nodes=2:blue:ppn=2+red:ppn=3+node-01", //7
		};
		for(String o : opts)
		{
			System.err.println("testLuaDistributed-" + i);
			job = UtilsForTest.getSimpleJob("testLuaDistributed-" + i, "#PBS -l "+o+"\nhostname");
			testJobCompilesClient(job);
			i++;
		}
	}
	
	@Test
	public void testLuaJobArray() throws Exception {
		PBSJob job = UtilsForTest.getSimpleJob("yarn-2", "hostname");
		int[][] arrayIds = new int[][]{new int[]{0,1}};
		job.setArrayTaskIds(arrayIds, 1);
		LuaApplicationMasterParameters lamp = testJobCompilesMaster(job);
		assertEquals(2, lamp.getContainerLaunchParameters().size());
		List<ContainerLaunchParameters> clpI = lamp.getContainerLaunchParameters();
		TIntHashSet yetToSeeArrayIds = new TIntHashSet();
		for(int i : JobUtils.getTaskArrayItems(arrayIds))
		{
			yetToSeeArrayIds.add(i);
		}
		
		TIntHashSet seenArrayIds = new TIntHashSet();
		
		for(ContainerLaunchParameters clp : clpI)
		{
			for(String mapped : new String[]{"PATH", "HOME", "TZ", "LANG", "MAIL", "SHELL"})
			{
				if (System.getenv(mapped) != null)
					assertEquals("Bad job env for " + mapped,  System.getenv(mapped), clp.getEnvironment().get("PBS_O_" + mapped));
			}	
			int arrayId = Integer.parseInt(clp.getEnvironment().get("PBS_ARRAYID"));
			assertTrue(yetToSeeArrayIds.contains(arrayId));
			yetToSeeArrayIds.remove(arrayId);
			assertFalse(seenArrayIds.contains(arrayId));
			seenArrayIds.add(arrayId);
			
			assertEquals("1", clp.getEnvironment().get("PBS_JOBID"));
			assertEquals(JobUtils.DEFAULT_QUEUE,  clp.getEnvironment().get("PBS_O_QUEUE"));
			assertEquals(Utils.getHostname(), clp.getEnvironment().get("PBS_O_HOST"));
			assertEquals("PBS_BATCH", clp.getEnvironment().get("PBS_ENVIRONMENT"));
			assertEquals("1", clp.getEnvironment().get("PBS_CORES"));
			assertEquals("512", clp.getEnvironment().get("PBS_VMEM"));
			assertEquals("$(pwd)/tmp", clp.getEnvironment().get("TMP"));
		}
	}
	
	@Test
	public void testLuaJobComplexEnvValues() throws Exception {
		PBSJob job = UtilsForTest.getSimpleJob("testHostname", "hostname");
		job.getVariable_List().put("WITH_QUOTES", "\"");
		job.getVariable_List().put("WITH_TAB", "\t");
		job.getVariable_List().put("BASH_FUNC_module", "() {  eval `/usr/bin/modulecmd bash $*`" + '\n' + "}");
		testJobCompilesClient(job);
	}

	@Test
	public void testLuaJobComplexEnvKey() throws Exception {
		PBSJob job = UtilsForTest.getSimpleJob("testHostname", "hostname");
		job.getVariable_List().put("BASH_FUNC_module()", "bla");
		testJobCompilesClient(job);
	}
	
}
