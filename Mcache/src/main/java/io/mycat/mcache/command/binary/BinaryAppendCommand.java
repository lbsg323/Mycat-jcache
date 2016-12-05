package io.mycat.mcache.command.binary;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mcache.McacheGlobalConfig;
import io.mycat.mcache.command.Command;
import io.mycat.mcache.conn.Connection;
import io.mycat.mcache.conn.handler.BinaryProtocol;

/**
 * append 命令
 * @author Bin.Lin
 * @creatDate 2016年12月4日
 * Request
 * MUST NOT have extras.
   MUST have key.
   MUST have value.
   Response:
   MUST NOT have extras.
   MUST NOT have key.
   MUST NOT have value.
   MUST have CAS
   These commands will either append or prepend the specified value to the requested key.
 */
public class BinaryAppendCommand implements Command{

	private static final Logger logger = LoggerFactory.getLogger(BinaryAppendCommand.class);
	@Override
	public void execute(Connection conn) throws IOException {
		logger.info("execute command append");
		// 检查 key的长度，key最大长度250个字节
		ByteBuffer key = readkey(conn);
		if (key.remaining() > McacheGlobalConfig.KEY_MAX_LENGTH) {
			writeResponse(conn, BinaryProtocol.OPCODE_SET,
					ProtocolResponseStatus.PROTOCOL_BINARY_RESPONSE_KEY_ENOENT.getStatus(), 1L);
		}
		ByteBuffer value = readValue(conn);
		// 检查value的长度
		if (value.remaining() > McacheGlobalConfig.VALUE_MAX_LENGTH) {
			writeResponse(conn, BinaryProtocol.OPCODE_SET,
					ProtocolResponseStatus.PROTOCOL_BINARY_RESPONSE_E2BIG.getStatus(), 1l);
		}
		// TODO Auto-generated method stub
		
	}

}
