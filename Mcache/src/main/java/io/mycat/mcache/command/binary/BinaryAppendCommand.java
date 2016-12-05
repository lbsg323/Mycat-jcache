package io.mycat.mcache.command.binary;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mcache.command.Command;
import io.mycat.mcache.conn.Connection;
import io.mycat.mcache.conn.handler.BinaryProtocol;

/**
 * append 命令
 * @author Bin.Lin
 * @creatDate 2016年12月5日
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

		int keylen = conn.getBinaryRequestHeader().getKeylen();
		int extlen = conn.getBinaryRequestHeader().getExtlen();
		ByteBuffer value = readValue(conn);
		// 检查参数合法性
		if (extlen == 0 && keylen > 0 && value.remaining() > 0) {
			ByteBuffer key = readkey(conn);
			String keystr = new String(cs.decode(key).array());
			logger.info("execute command append key {}", keystr);
			// TODO 调用memory append接口
			writeResponse(conn, BinaryProtocol.OPCODE_APPEND,
					ProtocolResponseStatus.PROTOCOL_BINARY_RESPONSE_SUCCESS.getStatus(), 1L);
		} else {
			writeResponse(conn, BinaryProtocol.OPCODE_APPEND,
					ProtocolResponseStatus.PROTOCOL_BINARY_RESPONSE_EINVAL.getStatus(), 0L);
		}
	}

}
