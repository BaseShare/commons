package com.sinoiov.yyzc.commons.kafka.producer.client;

import java.util.Date;

import com.sinoiov.yyzc.commons.kafka.YyzcKafkaBasicProducer;
import com.sinoiov.yyzc.commons.kafka.producer.message.MessageAccount;
import com.sinoiov.yyzc.commons.kafka.producer.message.MessageCallback;

public class MessageSendClient {
	
	
	public static void main(String[] args){
		YyzcKafkaBasicProducer client = new YyzcKafkaBasicProducer();
		for(int i=0;i<1;i++){
			try {
				TestClass t = new TestClass();
				t.setOperType("examine");
				t.setObjectName("com.sinoiov.crm.baseservice.personauth.beans.PersonAuthProcess");
				t.setSystemSign("com.sinoiov.crm.managerApp");
				SubClass s = new SubClass();
				s.setId("2eb6a9cc-da22-478c-b30c-f17c8742dc23");
				s.setPersonAuthId("d71c64fa-6012-4fcb-84f9-41d0136aec5c");
				s.setProcessId("7820a95e-f8bc-4bb1-8a13-46ba094944ab");
				s.setProcessResult("1");
				s.setProcessDescr("11");
				s.setCreateTime("1439023980791");
				s.setIsPhone("0");
				s.setResult("111");
				s.setCustomerId("1ba6b350-6c3d-49c1-8428-fb007f64597e");
				s.setRefuseTypeCode(1);
				s.setRefuseTypeName("丁小帅测试");
				t.setDataObject(s);
				client.send("CRM-SERVICE-PERSONAUTH-EXAMINE", t);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
