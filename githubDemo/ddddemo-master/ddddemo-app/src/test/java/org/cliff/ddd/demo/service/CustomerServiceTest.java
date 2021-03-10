package org.cliff.ddd.demo.service;

import org.cliff.ddd.demo.common.AppConstants;
import org.cliff.ddd.demo.common.BizCode;
import org.cliff.ddd.demo.dto.CustomerCheckConflictCmd;
import org.cliff.ddd.demo.dto.CustomerFindByCriteriaQry;
import org.cliff.ddd.demo.dto.clientobject.CustomerType;
import com.alibaba.sofa.context.TenantContext;
import com.alibaba.sofa.dto.MultiResponse;
import org.cliff.ddd.demo.api.CustomerServiceI;
import org.cliff.ddd.demo.dto.CustomerAddCmd;
import org.cliff.ddd.demo.dto.clientobject.CustomerCO;
import com.alibaba.sofa.dto.Response;
import com.alibaba.sofa.exception.BasicErrorCode;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;


/**
 * For TDD development, Recommend use {@link AppTestsContainer} to make test agile
 * 
 * @author fulan.zjf 2017年10月27日 下午3:26:11
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(locations={"classpath:applicationContext.xml"}) 
public class CustomerServiceTest {

    @Autowired
    private CustomerServiceI customerService;  

    @Test
    public void testCustomerAddSuccess( ) {
        //1.Prepare
    	TenantContext.set("10001", BizCode.DD);
    	CustomerAddCmd cmd = new CustomerAddCmd();
    	CustomerCO customerCO = new CustomerCO();
    	customerCO.setCustomerName("alibaba");
    	customerCO.setCustomerType(CustomerType.VIP);
    	cmd.setCustomer(customerCO);

    	//2.Execute
    	Response response = customerService.addCustomer(cmd);

    	//3.Expect
        Assert.assertTrue(response.isSuccess());
    }

    @Test
    public void testCustomerAddValidationFail( ) {
        //1.Prepare
        TenantContext.set("10001", BizCode.CGS);
        CustomerAddCmd cmd = new CustomerAddCmd();
        CustomerCO customerCO = new CustomerCO();
        customerCO.setCustomerName("alibaba");
        customerCO.setCustomerType(CustomerType.VIP);
        cmd.setCustomer(customerCO);

        //2.Execute
        Response response = customerService.addCustomer(cmd);

        //3.Expect
        Assert.assertFalse(response.isSuccess());
        Assert.assertEquals(response.getErrCode(), BasicErrorCode.BIZ_ERROR.getErrCode());
    }

    @Test
    public void testCustomerAddRuleVoilation( ){
        //1.Prepare
        TenantContext.set("10001", BizCode.DD);
        CustomerAddCmd cmd = new CustomerAddCmd();
        CustomerCO customerCO = new CustomerCO();
        customerCO.setCustomerName("alibaba");
        customerCO.setCustomerType(CustomerType.VIP);
        customerCO.setSource(AppConstants.SOURCE_AD);
        cmd.setCustomer(customerCO);

        //2.Execute
        Response response = customerService.addCustomer(cmd);

        //3.Expect
        Assert.assertFalse(response.isSuccess());
        Assert.assertEquals(response.getErrCode(), BasicErrorCode.BIZ_ERROR.getErrCode());
    }
}
