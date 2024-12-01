/**
 * 
 */
package net.sdroses.jdbcutils;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

/**
 * 
 */
@SpringBootTest
@AutoConfigureMockMvc
public class JDBCUtilsControllerTest {
	
	@Autowired
	private MockMvc mvc;
	
	@Test
	public void getTableDefinition() throws Exception {
		mvc.perform(MockMvcRequestBuilders.get("/adhoc/describe/..TEST_TABLE").accept(MediaType.TEXT_PLAIN))
		.andExpect(status().isOk())
		.andExpect(content().string(startsWith("`TEST_TABLE` (ID")));
	}
}
