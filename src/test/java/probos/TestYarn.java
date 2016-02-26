package probos;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.yarn.api.impl.pb.client.ContainerManagementProtocolPBClientImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerLaunchContextProto;
import org.apache.hadoop.yarn.proto.YarnProtos.StringStringMapProto;
import org.junit.Test;

public class TestYarn {

	ContainerManagementProtocolPBClientImpl i;
	
	@Test public void context()
	{
		for (int id : new int[]{1,2,3,4,5,6})
		{
			ContainerLaunchContextProto.Builder builder = ContainerLaunchContextProto.newBuilder();
			final Map<String, String> environment = new HashMap<String,String>();
			//1=function: 6c9f5c0d, PBS_CORES=1, PBS_VMEM=512, PBS_ARRAYID=5, TMP=$(pwd)/tmp
			environment.put("1", "function: 6c9f5c0d");
			environment.put("PBS_CORES", String.valueOf(id));
			environment.put("PBS_VMEM", "512");
			environment.put("PBS_ARRAYID", "5");
			environment.put("TMP", "$(pwd)/tmp");
			builder.clearEnvironment();
			Iterable<StringStringMapProto> iterable = 
			        new Iterable<StringStringMapProto>() {
			      
			      @Override
			      public Iterator<StringStringMapProto> iterator() {
			        return new Iterator<StringStringMapProto>() {
			          
			          Iterator<String> keyIter = environment.keySet().iterator();
			          
			          @Override
			          public void remove() {
			            throw new UnsupportedOperationException();
			          }
			          
			          @Override
			          public StringStringMapProto next() {
			            String key = keyIter.next();
			            return StringStringMapProto.newBuilder().setKey(key).setValue(
			                (environment.get(key))).build();
			          }
			          
			          @Override
			          public boolean hasNext() {
			            return keyIter.hasNext();
			          }
			        };
			      }
			    };
			builder.addAllEnvironment(iterable);
		}
	}
}
