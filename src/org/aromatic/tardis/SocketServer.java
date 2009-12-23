package org.aromatic.tardis;

import org.xsocket.*;
import org.xsocket.connection.*;
import java.io.*;
import java.nio.BufferUnderflowException;
import java.nio.channels.ClosedChannelException;
import java.util.*;
import java.math.*;

public class SocketServer implements IConnectionScoped,
    IDataHandler, IConnectHandler, IDisconnectHandler
{
    protected static boolean DEBUG = false;

    protected static final String EOL = "\r\n";
    protected static final int VARIABLE = Integer.MAX_VALUE;
    protected static File tardisFile = new File("tardis.db");
    protected static IServer srv = null;

    protected static final Map<String, Integer> commands = 
	    new HashMap<String, Integer>();

    protected static int connected = 0;

    protected int selected;

    @Override
    public Object clone() throws CloneNotSupportedException {
        return new SocketServer();
    }

    // Constructor
    SocketServer()
    {
        selected = 0;
    }
	    
    public static void main(String[] args)
    {
	if (args.length > 0)
		DEBUG = true;

	commands.put("",	    0);
    	commands.put("PING",        0);
    	commands.put("QUIT",        0);
    	commands.put("SHUTDOWN",    0);
	commands.put("SAVE",        0);
	commands.put("BGSAVE",      0);
	commands.put("BGREWRITEAOF",0);
	commands.put("LASTSAVE",    0);

    	commands.put("SET",         2);
    	commands.put("GET",         1);
    	commands.put("GETSET",      2);
    	commands.put("MGET",        VARIABLE);
    	commands.put("SETNX",       2);
    	commands.put("MSET",        VARIABLE);
    	commands.put("MSETNX",      VARIABLE);
    	commands.put("INCR",        1);
    	commands.put("INCRBY",      2);
    	commands.put("DECR",        1);
    	commands.put("DECRBY",      2);
    	commands.put("EXISTS",      1);
    	commands.put("DEL",         VARIABLE);
    	commands.put("TYPE",        1);

    	commands.put("KEYS",        1);
    	commands.put("RANDOMKEY",   0);
    	commands.put("RENAME",      2);
    	commands.put("RENAMENX",    2);
    	commands.put("DBSIZE",      0);
    	commands.put("EXPIRE",      2);
    	commands.put("EXPIREAT",    2);
    	commands.put("TTL",         1);

	commands.put("RPUSH",       2);
	commands.put("LPUSH",       2);
	commands.put("LLEN",        1);
	commands.put("LRANGE",      3);
	commands.put("LTRIM",       3);
	commands.put("LINDEX",      2);
	commands.put("LSET",        3);
	commands.put("LREM",        3);
	commands.put("LPOP",        1);
	commands.put("RPOP",        1);
	commands.put("RPOPLPUSH",   2);

	commands.put("SADD",        2);
	commands.put("SREM",        2);
	commands.put("SPOP",        1);
	commands.put("SMOVE",       3);
        commands.put("SCARD",       1);
	commands.put("SISMEMBER",   2);
	commands.put("SMEMBERS",    1);
	commands.put("SRANDMEMBER", 1);
	commands.put("SINTER",      VARIABLE);
	commands.put("SINTERSTORE", VARIABLE);
	commands.put("SUNION",      VARIABLE);
	commands.put("SUNIONSTORE", VARIABLE);
	commands.put("SDIFF",       VARIABLE);
	commands.put("SDIFFSTORE",  VARIABLE);

	commands.put("ZADD",        3);
	commands.put("ZINCRBY",     3);
	commands.put("ZREM",        2);
	commands.put("ZRANGE",      3);
	commands.put("ZREVRANGE",   3);
	commands.put("ZREMRANGEBYSCORE", 3);
	commands.put("ZRANGEBYSCORE",    VARIABLE);
	commands.put("ZCARD",       1);
	commands.put("ZSCORE",      2);

	commands.put("SORT",        VARIABLE);

	commands.put("SELECT",      1);
	commands.put("MOVE",        2);
	commands.put("FLUSHDB",     0);
	commands.put("FLUSHALL",    0);

	commands.put("INFO",        0);
	commands.put("DEBUG",       1);

	    if (tardisFile.exists()) {
		System.out.println("loading data from " + tardisFile);
		try {
		    Tardis.load(tardisFile);
		} catch(Exception e) {
            	    System.out.println(e);
		}
	    }

        try {
            srv = new Server(6379, new SocketServer());
            srv.run();

	    System.out.println("saving data to " + tardisFile);
	    Tardis.save(tardisFile);
        } catch(Exception ex) {
            System.out.println(ex);
        }
    }

    protected static void shutdownServer()
    {
        try {
            srv.close();
        } catch(Exception ex) {
            System.out.println(ex);
        }       
    }


    public boolean onConnect(INonBlockingConnection nbc) 
        throws IOException, BufferUnderflowException, MaxReadSizeExceededException
    {
        synchronized(srv) {
            connected++;
        }

	System.out.printf("%d clients connected\n", connected);
        return true;
    }

    public boolean onDisconnect(INonBlockingConnection nbc) throws IOException
    {
        synchronized(srv) {
            connected--;
        }

	System.out.printf("%d clients connected\n", connected);
	return true;
    }

    public boolean onData(INonBlockingConnection nbc) 
	  throws IOException, BufferUnderflowException, 
	  ClosedChannelException, MaxReadSizeExceededException
    {
	nbc.markReadPosition();

	try {
	    String data = nbc.readStringByDelimiter(EOL);

	    if (DEBUG)
		System.out.println("req: " + data);

	    String args[] = {};
	    String cmd = "";

	    if (data.startsWith("*")) {
		List<String> list = parseList(nbc, data);
		if (list !=  null) {
		    cmd = list.remove(0).toUpperCase();
		    args = list.toArray(new String[0]);
		}
	    } else {
	        args = data.trim().split(" ", 2);
	        cmd = args[0].toUpperCase();

	        if (args.length > 1)
	    	    args = args[1].split(" ");
	        else
	            args = new String[0];
	    }

	    Integer n = commands.get(cmd);
	    if (DEBUG)
		System.out.println("cmd: " + cmd + ", params: " + n);

	    Tardis tardis = Tardis.select(selected);

	    if (n == null)
		printError(nbc, "unknown command");

	    else if (n != VARIABLE && n != args.length) {
	        if (DEBUG) {
		    System.out.println("command: " + cmd);
		    System.out.println("args: " + args.length);
		}

		printError(nbc, "wrong number of arguments for '" 
		    + cmd.toLowerCase() + "' command");
	    }

	    else if (cmd.equals("")) {
		//printEmpty(nbc);
	    }

	    else if (cmd.equals("PING"))
		printStatus(nbc, "PONG");

	    else if (cmd.equals("SHUTDOWN"))
		shutdownServer();

	    else if (cmd.equals("SAVE") 
		|| cmd.equals("BGSAVE")
		|| cmd.equals("BGREWRITEAOF")) {
		try {
		    Tardis.save(tardisFile);

		    if (cmd.equals("BGSAVE"))
			printStatus(nbc, "Background saving started");
		    else if (cmd.equals("BGREWRITEAOF"))
			printStatus(nbc, "Background append only file rewriting started");
		    else
			printStatus(nbc);
		} catch(Exception e) {
		    e.printStackTrace(System.out);
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("LASTSAVE")) {
		printInteger(nbc, tardisFile.lastModified());
	    }

	    else if (cmd.equals("QUIT"))
		nbc.close();

	    //
	    // STRING COMMANDS
	    //

	    else if (cmd.equals("SET")) {
		String key = args[0];
		String value = parseString(nbc, args[1]);

		tardis.set(key, value);
		printStatus(nbc);
	    }

	    else if (cmd.equals("GET")) {
		String key = args[0];

		try {
		    String value = tardis.get(key);
		    printResult(nbc, value);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("GETSET")) {
		String key = args[0];
	        String value = parseString(nbc, args[1]);

		try {
		    String result = tardis.getset(key, value);
		    printResult(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("MGET")) {
		List<String> values = tardis.mget(args);
		printList(nbc, values);
	    }

	    else if (cmd.equals("SETNX")) {
		String key = args[0];
	        String value = parseString(nbc, args[1]);

		boolean result = tardis.setnx(key, value);
		printInteger(nbc, result);
	    }

	    else if (cmd.equals("MSET")) {
		if (args.length % 2 != 0)
			printError(nbc, "wrong number of arguments");
		else {
			tardis.mset(args);
			printStatus(nbc);
		}
	    }

	    else if (cmd.equals("MSETNX")) {
		if (args.length % 2 != 0)
			printError(nbc, "wrong number of arguments");
		else {
			boolean result = tardis.msetnx(args);
			printInteger(nbc, result);
		}
	    }

	    else if (cmd.equals("INCR")) {
		String key = args[0];

		try {
		    long value = tardis.incr(key);
		    printInteger(nbc, value);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("INCRBY")) {
		String key = args[0];
		long step = parseLong(args[1]);

		try {
		    long value = tardis.incrby(key, step);
		    printInteger(nbc, value);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("DECR")) {
		String key = args[0];

		try {
		    long value = tardis.decr(key);
		    printInteger(nbc, value);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("DECRBY")) {
		String key = args[0];
		long step = parseLong(args[1]);

		try {
		    long value = tardis.decrby(key, step);
		    printInteger(nbc, value);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("EXISTS")) {
		String key = args[0];

		boolean result = tardis.exists(key);
		printInteger(nbc, result);
	    }

	    else if (cmd.equals("DEL")) {
		int result = tardis.del(args);
		printInteger(nbc, result);
	    }

	    else if (cmd.equals("TYPE")) {
		String key = args[0];

		String result = tardis.type(key);
		printResult(nbc, result);
	    }

	    //
	    // KEY SPACE COMMANDS
	    //

	    else if (cmd.equals("KEYS")) {
		String pattern = args[0];

		String result = tardis.keys(pattern);
		printResult(nbc, result);
	    }

	    else if (cmd.equals("RANDOMKEY")) {
		String result = tardis.randomkey();
		printResult(nbc, result);
	    }

	    else if (cmd.equals("RENAME")) {
		String oldname = args[0];
		String newname = args[1];

		try {
		    tardis.rename(oldname, newname);
		    printStatus(nbc);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("RENAMENX")) {
		String oldname = args[0];
		String newname = args[1];

		try {
		    boolean result = tardis.renamenx(oldname, newname);
		    printInteger(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("DBSIZE")) {
		long result = tardis.dbsize();
		printInteger(nbc, result);
	    }

	    else if (cmd.equals("EXPIRE")) {
		String key = args[0];
		long time = parseLong(args[1]) * 1000;

		boolean result = tardis.expireat(key, System.currentTimeMillis()+time);
		printInteger(nbc, result);
	    }

	    else if (cmd.equals("EXPIREAT")) {
		String key = args[0];
		long time = parseLong(args[1]) * 1000;

		boolean result = tardis.expireat(key, time);
		printInteger(nbc, result);
	    }

	    else if (cmd.equals("TTL")) {
		String key = args[0];

		long result = tardis.ttl(key);
		if (result > 0)
			result = (result+499)/1000;

		printInteger(nbc, result);
	    }

	    //
	    // LIST COMMANDS
	    //

	    else if (cmd.equals("RPUSH")) {
		String key = args[0];
		String value = parseString(nbc, args[1]);

		try {
		    tardis.rpush(key, value);
		    printStatus(nbc);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("LPUSH")) {
		String key = args[0];
		String value = parseString(nbc, args[1]);

		try {
		    tardis.lpush(key, value);
		    printStatus(nbc);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("LLEN")) {
		String key = args[0];

		try {
		    int result = tardis.llen(key);
		    printInteger(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("LRANGE")) {
		String key = args[0];
		int start = parseInteger(args[1]);
		int end = parseInteger(args[2]);

		try {
		    List<String> values = tardis.lrange(key, start, end);
		    printList(nbc, values);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("LTRIM")) {
		String key = args[0];
		int start = parseInteger(args[1]);
		int end = parseInteger(args[2]);

		try {
		    tardis.ltrim(key, start, end);
		    printStatus(nbc);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("LINDEX")) {
		String key = args[0];
		int index = parseInteger(args[1]);

		try {
		    String result = tardis.lindex(key, index);
		    printResult(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("LSET")) {
		String key = args[0];
		int index = parseInteger(args[1]);
		String value = parseString(nbc, args[2]);

		try {
		    tardis.lset(key, index, value);
		    printStatus(nbc);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("LREM")) {
		String key = args[0];
		int count = parseInteger(args[1]);
		String value = parseString(nbc, args[2]);

		try {
		    int result = tardis.lrem(key, count, value);
		    printInteger(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("LPOP")) {
		String key = args[0];

		try {
		    String result = tardis.lpop(key);
		    printResult(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("RPOP")) {
		String key = args[0];

		try {
		    String result = tardis.rpop(key);
		    printResult(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("RPOPLPUSH")) {
		String src = args[0];
		String dest = parseString(nbc, args[1]);

		try {
		    String result = tardis.rpoplpush(src, dest);
		    printResult(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    //
	    // SETS COMMANDS
	    //

	    else if (cmd.equals("SADD")) {
	    	String key = args[0];
	    	String member = parseString(nbc, args[1]);

		try {
		    boolean result = tardis.sadd(key, member);
		    printInteger(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("SREM")) {
	    	String key = args[0];
	    	String member = parseString(nbc, args[1]);

		try {
		    boolean result = tardis.srem(key, member);
		    printInteger(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

            else if (cmd.equals("SPOP")) {
	    	String key = args[0];

		try {
		    String result = tardis.spop(key);
		    printResult(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("SMOVE")) {
	    	String src = args[0];
	    	String dest = args[1];
	    	String member = parseString(nbc, args[2]);

		try {
		    boolean result = tardis.smove(src, dest, member);
		    printInteger(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

            else if (cmd.equals("SCARD")) {
	    	String key = args[0];

		try {
		    int result = tardis.scard(key);
		    printInteger(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("SISMEMBER")) {
	    	String key = args[0];
	    	String member = parseString(nbc, args[1]);

		try {
		    boolean result = tardis.sismember(key, member);
		    printInteger(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("SMEMBERS")) {
		try {
		    List<String> values = tardis.sinter(args);
		    printList(nbc, values);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("SRANDMEMBER")) {
	    	String key = args[0];

		try {
		    String result = tardis.srandmember(key);
		    printResult(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("SINTER")) {
		try {
		    List<String> values = tardis.sinter(args);;
		    printList(nbc, values);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("SINTERSTORE")) {
		try {
		    int result = tardis.sinterstore(args);;
		    printInteger(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("SUNION")) {
		try {
		    List<String> values = tardis.sunion(args);;
		    printList(nbc, values);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("SUNIONSTORE")) {
		try {
		    int result = tardis.sunionstore(args);;
		    printInteger(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("SDIFF")) {
		try {
		    List<String> values = tardis.sdiff(args);;
		    printList(nbc, values);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("SDIFFSTORE")) {
		try {
		    int result = tardis.sdiffstore(args);;
		    printInteger(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    //
	    // ZSETS COMMANDS
	    //

	    else if (cmd.equals("ZADD")) {
	    	String key = args[0];
	    	String score = args[1];
	    	String member = parseString(nbc, args[2]);

		try {
		    boolean result = tardis.zadd(key, score, member);
		    printInteger(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("ZINCRBY")) {
	    	String key = args[0];
	    	String score = args[1];
	    	String member = parseString(nbc, args[2]);

		try {
		    double result = tardis.zincrby(key, score, member);
		    printDouble(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("ZREM")) {
	    	String key = args[0];
	    	String member = parseString(nbc, args[1]);

		try {
		    boolean result = tardis.zrem(key, member);
		    printInteger(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("ZRANGE")) {
		String key = args[0];
		int start = parseInteger(args[1]);
		int end = parseInteger(args[2]);

		try {
		    List<String> values = tardis.zrange(key, start, end, false);
		    printList(nbc, values);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("ZREVRANGE")) {
		String key = args[0];
		int start = parseInteger(args[1]);
		int end = parseInteger(args[2]);

		try {
		    List<String> values = tardis.zrange(key, start, end, true);
		    printList(nbc, values);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("ZRANGEBYSCORE")) {
		try {
	           String key = args[0];
		   String min = args[1];
		   String max = args[2];
		   int offset = 0;
		   int count = Integer.MAX_VALUE;

		    for (int i=3; i < args.length; i++) {
			String arg = args[i];

			if ("LIMIT".equalsIgnoreCase(arg)) {
			    offset = parseInteger(args[++i]);
			    count = parseInteger(args[++i]);
			} else
        	            throw new UnsupportedOperationException(Tardis.SYNTAX);
		    }
		
		    List<String> values = tardis.zrangebyscore(key, min, max, offset, count);
		    printList(nbc, values);
		} catch(ArrayIndexOutOfBoundsException e) {
		    printError(nbc, Tardis.SYNTAX);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("ZREMRANGEBYSCORE")) {
		String key = args[0];
		String min = args[1];
		String max = args[2];

		try {
		    int result = tardis.zremrangebyscore(key, min, max);
		    printInteger(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

            else if (cmd.equals("ZCARD")) {
	    	String key = args[0];

		try {
		    int result = tardis.zcard(key);
		    printInteger(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

            else if (cmd.equals("ZSCORE")) {
	    	String key = args[0];
	    	String member = parseString(nbc, args[1]);

		try {
		    String result = tardis.zscore(key, member);
		    printResult(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    //
	    // SORT
	    //

	    else if (cmd.equals("SORT")) {
	        String key = null;
		boolean asc = true;
		boolean alpha = false;
		String pattern_by = null;
		String pattern_get = null;
		int start = 0;
		int count = Integer.MAX_VALUE;

		try {
		    for (int i=0; i < args.length; i++) {
			String arg = args[i];

			if (i == 0)
			    key = arg;
			else if ("ASC".equalsIgnoreCase(arg))
			    asc = true;
			else if ("DESC".equalsIgnoreCase(arg))
			    asc = false;
			else if ("ALPHA".equalsIgnoreCase(arg))
			    alpha = true;
			else if ("BY".equalsIgnoreCase(arg))
			    pattern_by = args[++i];
			else if ("GET".equalsIgnoreCase(arg))
			    pattern_get = args[++i];
			else if ("LIMIT".equalsIgnoreCase(arg)) {
			    start = parseInteger(args[++i]);
			    count = parseInteger(args[++i]);
			} else
        	            throw new UnsupportedOperationException(Tardis.SYNTAX);
		    }

		    List<String> values = tardis.sort(key, asc, alpha, start, count, pattern_by, pattern_get);
		    printList(nbc, values);
		} catch(ArrayIndexOutOfBoundsException e) {
		    printError(nbc, Tardis.SYNTAX);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    //
	    // MULTIPLE DB COMMANDS
	    //
	    
	    else if (cmd.equals("SELECT")) {
		int index = parseInteger(args[0]);

		try {
		    Tardis.select(index);
		    selected = index;
		    printStatus(nbc);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("MOVE")) {
		String key = args[0];
		int index = parseInteger(args[1]);

		try {
		    boolean result = tardis.move(key, index);
		    printInteger(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("FLUSHDB")) {
	        tardis.flushdb();
		printStatus(nbc);
	    }

	    else if (cmd.equals("FLUSHALL")) {
	        Tardis.flushall();
		printStatus(nbc);
	    }

	    else if (cmd.equals("INFO")) {
		printResult(nbc, "version:"
			+ Tardis.V_MAJOR
			+ "."
			+ Tardis.V_MINOR);
	    }

	    else if (cmd.equals("DEBUG")) {
		cmd = args[0];
		if (cmd.equalsIgnoreCase("RELOAD")) {
		    try {
		    	Tardis.save(tardisFile);
	        	Tardis.flushall();
			Tardis.load(tardisFile);

		        printStatus(nbc);
		    } catch(Exception e) {
		        printError(nbc, e.getMessage());
		    }
		}

		else
		    printStatus(nbc);
	    }

	    nbc.removeReadMark();
	} catch(UnsupportedOperationException e1) {
	    printError(nbc, e1.getMessage());
	} catch(BufferUnderflowException e2) {
	    nbc.resetToReadMark();
	} catch(ClosedChannelException e3) {
	    System.out.println("connection closed");
	} catch(Exception e4) {
	    e4.printStackTrace(System.out);
	}
	 
	return true;
    }

    public int parseInteger(String v)
    {
	try {
	    return Integer.parseInt(v);
	} catch(Exception e) {
	    return 0;
	}
    }

    public long parseLong(String v)
    {
	try {
	    return Long.parseLong(v);
	} catch(Exception e) {
	    return 0;
	}
    }

    public String parseString(INonBlockingConnection nbc, String v)
	  throws IOException, BufferUnderflowException
    {
	long len = parseInteger(v);
	if (len < 0 || len > 1024*1024*1024)
	    throw new UnsupportedOperationException("invalid bulk write count");

	byte result[] = nbc.readBytesByLength((int)len);
	
//System.out.printf("received %d bytes, hashCode=%d\n", 
//		result.length, Arrays.hashCode(result));
	nbc.readBytesByLength(2);
	return new String(result);
    }

    public List<String> parseList(INonBlockingConnection nbc, String v)
	  throws IOException, BufferUnderflowException
    {
	long n = parseInteger(v.substring(1));
	if (n <= 0)
		return null;

	List<String> result = new ArrayList<String>();

	for (int i=0; i < n; i++) {
	    v = nbc.readStringByDelimiter(EOL);
	    if (!v.startsWith("$"))
	        throw new UnsupportedOperationException("multi bulk protocol error");
		
	    v = parseString(nbc, v.substring(1));
	    result.add(v);
	}

	return result;
    }

    public static void printResult(INonBlockingConnection nbc, Object result) 
	    throws IOException
    {
	if (result == null)
	    nbc.write("$-1" + EOL);
	else {
	    byte v[] = result.toString().getBytes();
//System.out.printf("sent %d bytes, hashCode=%d\n", 
//		v.length, Arrays.hashCode(v));
	    nbc.write("$" + v.length + EOL);
	    nbc.write(v);
	    nbc.write(EOL);
	}
    }

    public static void printEmpty(INonBlockingConnection nbc)
	    throws IOException
    {
	nbc.write(EOL);
    }

    public static void printStatus(INonBlockingConnection nbc)
	    throws IOException
    {
	printStatus(nbc, "OK");
    }

    public static void printStatus(INonBlockingConnection nbc, String status)
	    throws IOException
    {
	nbc.write("+" + status + EOL);
    }

    public static void printError(INonBlockingConnection nbc, String error)
	    throws IOException
    {
        System.out.println("error: " + error);
	nbc.write("-ERR " + error + EOL);
    }

    public static void printDouble(INonBlockingConnection nbc, double value)
	    throws IOException
    {
	BigDecimal bd = (new BigDecimal(value)).round(MathContext.DECIMAL64);
	nbc.write(":" + bd.toString() + EOL);
    }

    public static void printInteger(INonBlockingConnection nbc, boolean value)
	    throws IOException
    {
	printInteger(nbc, value ? 1 : 0);
    }

    public static void printInteger(INonBlockingConnection nbc, long value)
	    throws IOException
    {
	nbc.write(":" + value + EOL);
    }

    public static void printList(INonBlockingConnection nbc, List<?> values)
	    throws IOException
    {
        if (values == null) {
	    nbc.write("*-1" + EOL);
        } else {
	    int size = values.size();
	    nbc.write("*" + size + EOL);

	    for (Object v : values)
		printResult(nbc, v);
	}
    }
}
