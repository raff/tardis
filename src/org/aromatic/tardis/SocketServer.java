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
    	commands.put("ping",        0);
    	commands.put("quit",        0);
    	commands.put("shutdown",    0);
	commands.put("save",        0);
	commands.put("bgsave",      0);
	commands.put("bgrewriteaof",0);
	commands.put("lastsave",    0);

    	commands.put("set",         2);
    	commands.put("get",         1);
    	commands.put("getset",      2);
    	commands.put("mget",        VARIABLE);
    	commands.put("setnx",       2);
    	commands.put("mset",        VARIABLE);
    	commands.put("msetnx",      VARIABLE);
    	commands.put("incr",        1);
    	commands.put("incrby",      2);
    	commands.put("decr",        1);
    	commands.put("decrby",      2);
    	commands.put("exists",      1);
    	commands.put("del",         VARIABLE);
    	commands.put("type",        1);

    	commands.put("keys",        1);
    	commands.put("randomkey",   0);
    	commands.put("rename",      2);
    	commands.put("renamenx",    2);
    	commands.put("dbsize",      0);
    	commands.put("expire",      2);
    	commands.put("expireat",    2);
    	commands.put("ttl",         1);

	commands.put("rpush",       2);
	commands.put("lpush",       2);
	commands.put("llen",        1);
	commands.put("lrange",      3);
	commands.put("ltrim",       3);
	commands.put("lindex",      2);
	commands.put("lset",        3);
	commands.put("lrem",        3);
	commands.put("lpop",        1);
	commands.put("rpop",        1);
	commands.put("rpoplpush",   2);

	commands.put("sadd",        2);
	commands.put("srem",        2);
	commands.put("spop",        1);
	commands.put("smove",       3);
        commands.put("scard",       1);
	commands.put("sismember",   2);
	commands.put("smembers",    1);
	commands.put("srandmember", 1);
	commands.put("sinter",      VARIABLE);
	commands.put("sinterstore", VARIABLE);
	commands.put("sunion",      VARIABLE);
	commands.put("sunionstore", VARIABLE);
	commands.put("sdiff",       VARIABLE);
	commands.put("sdiffstore",  VARIABLE);

	commands.put("zadd",        3);
	commands.put("zincrby",     3);
	commands.put("zrem",        2);
	commands.put("zrange",      VARIABLE);
	commands.put("zrevrange",   VARIABLE);
	commands.put("zremrangebyscore", 3);
	commands.put("zrangebyscore",    VARIABLE);
	commands.put("zcard",       1);
	commands.put("zscore",      2);

	commands.put("sort",        VARIABLE);

	commands.put("select",      1);
	commands.put("move",        2);
	commands.put("flushdb",     0);
	commands.put("flushall",    0);

	commands.put("info",        0);
	commands.put("debug",       1);

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
		    cmd = list.remove(0).toLowerCase();
		    args = list.toArray(new String[0]);
		}
	    } else {
	        args = data.trim().split(" ", 2);
	        cmd = args[0].toLowerCase();

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
		    + cmd + "' command");
	    }

	    else if (cmd.equals("")) {
		//printEmpty(nbc);
	    }

	    else if (cmd.equals("ping"))
		printStatus(nbc, "PONG");

	    else if (cmd.equals("shutdown"))
		shutdownServer();

	    else if (cmd.equals("save") 
		|| cmd.equals("bgsave")
		|| cmd.equals("bgrewriteaof")) {
		try {
		    Tardis.save(tardisFile);

		    if (cmd.equals("bgsave"))
			printStatus(nbc, "Background saving started");
		    else if (cmd.equals("bgrewriteaof"))
			printStatus(nbc, "Background append only file rewriting started");
		    else
			printStatus(nbc);
		} catch(Exception e) {
		    e.printStackTrace(System.out);
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("lastsave")) {
		printInteger(nbc, tardisFile.lastModified());
	    }

	    else if (cmd.equals("quit"))
		nbc.close();

	    //
	    // STRING COMMANDS
	    //

	    else if (cmd.equals("set")) {
		String key = args[0];
		String value = parseString(nbc, args[1]);

		tardis.set(key, value);
		printStatus(nbc);
	    }

	    else if (cmd.equals("get")) {
		String key = args[0];

		try {
		    String value = tardis.get(key);
		    printResult(nbc, value);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("getset")) {
		String key = args[0];
	        String value = parseString(nbc, args[1]);

		try {
		    String result = tardis.getset(key, value);
		    printResult(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("mget")) {
		List<String> values = tardis.mget(args);
		printList(nbc, values);
	    }

	    else if (cmd.equals("setnx")) {
		String key = args[0];
	        String value = parseString(nbc, args[1]);

		boolean result = tardis.setnx(key, value);
		printInteger(nbc, result);
	    }

	    else if (cmd.equals("mset")) {
		if (args.length % 2 != 0)
			printError(nbc, "wrong number of arguments");
		else {
			tardis.mset(args);
			printStatus(nbc);
		}
	    }

	    else if (cmd.equals("msetnx")) {
		if (args.length % 2 != 0)
			printError(nbc, "wrong number of arguments");
		else {
			boolean result = tardis.msetnx(args);
			printInteger(nbc, result);
		}
	    }

	    else if (cmd.equals("incr")) {
		String key = args[0];

		try {
		    long value = tardis.incr(key);
		    printInteger(nbc, value);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("incrby")) {
		String key = args[0];
		long step = parseLong(args[1]);

		try {
		    long value = tardis.incrby(key, step);
		    printInteger(nbc, value);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("decr")) {
		String key = args[0];

		try {
		    long value = tardis.decr(key);
		    printInteger(nbc, value);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("decrby")) {
		String key = args[0];
		long step = parseLong(args[1]);

		try {
		    long value = tardis.decrby(key, step);
		    printInteger(nbc, value);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("exists")) {
		String key = args[0];

		boolean result = tardis.exists(key);
		printInteger(nbc, result);
	    }

	    else if (cmd.equals("del")) {
		int result = tardis.del(args);
		printInteger(nbc, result);
	    }

	    else if (cmd.equals("type")) {
		String key = args[0];

		String result = tardis.type(key);
		printResult(nbc, result);
	    }

	    //
	    // KEY SPACE COMMANDS
	    //

	    else if (cmd.equals("keys")) {
		String pattern = args[0];

		String result = tardis.keys(pattern);
		printResult(nbc, result);
	    }

	    else if (cmd.equals("randomkey")) {
		String result = tardis.randomkey();
		printResult(nbc, result);
	    }

	    else if (cmd.equals("rename")) {
		String oldname = args[0];
		String newname = args[1];

		try {
		    tardis.rename(oldname, newname);
		    printStatus(nbc);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("renamenx")) {
		String oldname = args[0];
		String newname = args[1];

		try {
		    boolean result = tardis.renamenx(oldname, newname);
		    printInteger(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("dbsize")) {
		long result = tardis.dbsize();
		printInteger(nbc, result);
	    }

	    else if (cmd.equals("expire")) {
		String key = args[0];
		long time = parseLong(args[1]) * 1000;

		boolean result = tardis.expireat(key, System.currentTimeMillis()+time);
		printInteger(nbc, result);
	    }

	    else if (cmd.equals("expireat")) {
		String key = args[0];
		long time = parseLong(args[1]) * 1000;

		boolean result = tardis.expireat(key, time);
		printInteger(nbc, result);
	    }

	    else if (cmd.equals("ttl")) {
		String key = args[0];

		long result = tardis.ttl(key);
		if (result > 0)
			result = (result+499)/1000;

		printInteger(nbc, result);
	    }

	    //
	    // LIST COMMANDS
	    //

	    else if (cmd.equals("rpush")) {
		String key = args[0];
		String value = parseString(nbc, args[1]);

		try {
		    tardis.rpush(key, value);
		    printStatus(nbc);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("lpush")) {
		String key = args[0];
		String value = parseString(nbc, args[1]);

		try {
		    tardis.lpush(key, value);
		    printStatus(nbc);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("llen")) {
		String key = args[0];

		try {
		    int result = tardis.llen(key);
		    printInteger(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("lrange")) {
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

	    else if (cmd.equals("ltrim")) {
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

	    else if (cmd.equals("lindex")) {
		String key = args[0];
		int index = parseInteger(args[1]);

		try {
		    String result = tardis.lindex(key, index);
		    printResult(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("lset")) {
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

	    else if (cmd.equals("lrem")) {
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

	    else if (cmd.equals("lpop")) {
		String key = args[0];

		try {
		    String result = tardis.lpop(key);
		    printResult(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("rpop")) {
		String key = args[0];

		try {
		    String result = tardis.rpop(key);
		    printResult(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("rpoplpush")) {
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

	    else if (cmd.equals("sadd")) {
	    	String key = args[0];
	    	String member = parseString(nbc, args[1]);

		try {
		    boolean result = tardis.sadd(key, member);
		    printInteger(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("srem")) {
	    	String key = args[0];
	    	String member = parseString(nbc, args[1]);

		try {
		    boolean result = tardis.srem(key, member);
		    printInteger(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

            else if (cmd.equals("spop")) {
	    	String key = args[0];

		try {
		    String result = tardis.spop(key);
		    printResult(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("smove")) {
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

            else if (cmd.equals("scard")) {
	    	String key = args[0];

		try {
		    int result = tardis.scard(key);
		    printInteger(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("sismember")) {
	    	String key = args[0];
	    	String member = parseString(nbc, args[1]);

		try {
		    boolean result = tardis.sismember(key, member);
		    printInteger(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("smembers")) {
		try {
		    List<String> values = tardis.sinter(args);
		    printList(nbc, values);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("srandmember")) {
	    	String key = args[0];

		try {
		    String result = tardis.srandmember(key);
		    printResult(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("sinter")) {
		try {
		    List<String> values = tardis.sinter(args);;
		    printList(nbc, values);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("sinterstore")) {
		try {
		    int result = tardis.sinterstore(args);;
		    printInteger(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("sunion")) {
		try {
		    List<String> values = tardis.sunion(args);;
		    printList(nbc, values);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("sunionstore")) {
		try {
		    int result = tardis.sunionstore(args);;
		    printInteger(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("sdiff")) {
		try {
		    List<String> values = tardis.sdiff(args);;
		    printList(nbc, values);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("sdiffstore")) {
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

	    else if (cmd.equals("zadd")) {
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

	    else if (cmd.equals("zincrby")) {
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

	    else if (cmd.equals("zrem")) {
	    	String key = args[0];
	    	String member = parseString(nbc, args[1]);

		try {
		    boolean result = tardis.zrem(key, member);
		    printInteger(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("zrange")) {
		try {
		    String key = args[0];
		    int start = parseInteger(args[1]);
		    int end = parseInteger(args[2]);
		    boolean withscores = (args.length > 3 
			&& args[3].equalsIgnoreCase("WITHSCORES"));

		    List<String> values = tardis.zrange(
			key, start, end, false, withscores);

		    printList(nbc, values);
		} catch(ArrayIndexOutOfBoundsException e) {
		    printError(nbc, "wrong number of arguments for '" 
		        + args[0].toLowerCase() + "' command");
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("zrevrange")) {
		try {
		    String key = args[0];
		    int start = parseInteger(args[1]);
		    int end = parseInteger(args[2]);
		    boolean withscores = (args.length > 3 
			&& args[3].equalsIgnoreCase("WITHSCORES"));

		    List<String> values = tardis.zrange(
			key, start, end, true, withscores);
		    printList(nbc, values);
		} catch(ArrayIndexOutOfBoundsException e) {
		    printError(nbc, "wrong number of arguments for '" 
		        + args[0].toLowerCase() + "' command");
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("zrangebyscore")) {
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

	    else if (cmd.equals("zremrangebyscore")) {
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

            else if (cmd.equals("zcard")) {
	    	String key = args[0];

		try {
		    int result = tardis.zcard(key);
		    printInteger(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

            else if (cmd.equals("zscore")) {
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

	    else if (cmd.equals("sort")) {
	        String key = null;
		boolean asc = true;
		boolean alpha = false;
		String pattern_by = null;
		List<String> pattern_get = new ArrayList<String>();
		int start = 0;
		int count = Integer.MAX_VALUE;
		String store = null;

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
			    pattern_get.add(args[++i]);
			else if ("STORE".equalsIgnoreCase(arg))
                            store = args[++i];
			else if ("LIMIT".equalsIgnoreCase(arg)) {
			    start = parseInteger(args[++i]);
			    count = parseInteger(args[++i]);
			} else
        	            throw new UnsupportedOperationException(Tardis.SYNTAX);
		    }

		    List<String> values = tardis.sort(key, asc, alpha, start, count, pattern_by, pattern_get, store);
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
	    
	    else if (cmd.equals("select")) {
		int index = parseInteger(args[0]);

		try {
		    Tardis.select(index);
		    selected = index;
		    printStatus(nbc);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("move")) {
		String key = args[0];
		int index = parseInteger(args[1]);

		try {
		    boolean result = tardis.move(key, index);
		    printInteger(nbc, result);
		} catch(Exception e) {
		    printError(nbc, e.getMessage());
		}
	    }

	    else if (cmd.equals("flushdb")) {
	        tardis.flushdb();
		printStatus(nbc);
	    }

	    else if (cmd.equals("flushall")) {
	        Tardis.flushall();
		printStatus(nbc);
	    }

	    else if (cmd.equals("info")) {
		printResult(nbc, "version:"
			+ Tardis.V_MAJOR
			+ "."
			+ Tardis.V_MINOR);
	    }

	    else if (cmd.equals("debug")) {
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
