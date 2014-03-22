package io.core9.plugin.database.mongodb;

import java.nio.ByteBuffer;
import java.util.UUID;

public class GUID {

	/**
	 * Generate short UUID (13 characters)
	 * 
	 * @return short UUID
	 */
	private static String shortUUID() {
	  UUID uuid = UUID.randomUUID();
	  long l = ByteBuffer.wrap(uuid.toString().getBytes()).getLong();
	  return Long.toString(l, Character.MAX_RADIX).toUpperCase();
	}
	
	@SuppressWarnings("unused")
    private static String randomUUID(){
		return UUID.randomUUID().toString();
	}

	public static String getUUID() {
	    return shortUUID();
    }
}
