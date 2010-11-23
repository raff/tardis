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

    protected static String password = null;

    protected static final String EOL = "\r\n";
    protected static File tardisFile = new File("tardis.db");
    protected static IServer srv = null;

    protected static final int VARARGS = -1;

    protected static final Map<String, Command> commands = 
	    new HashMap<String, Command>();

    protected static int connected = 0;

    protected int selected;
    protected boolean multi = false;
    protected boolean authenticated = false;
    protected List<Request> requests = new ArrayList<Request>();

    @Override
    public Object clone() throws CloneNotSupportedException {
        return new SocketServer();
    }

    // Constructor
    SocketServer()
    {
        selected = 0;
    }

    private static class Command {
	public int nArgs = 0;
	public boolean stringArg = false;

	public String toString() {
	    return this.getClass().getName() + " - nArgs:" + nArgs + ", stringArg:" + stringArg;
        }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	}
    }

    //
    // SERVER COMMANDS
    //

    private static class NopCommand extends Command {
	NopCommand() { nArgs = 0; }
    }

    private static class PingCommand extends Command {
	PingCommand() { nArgs = 0; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    printStatus(nbc, "PONG");
	}
    }
	    
    private static class QuitCommand extends Command {
	QuitCommand() { nArgs = 0; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    nbc.close();
	}
    }

    private static class ShutdownCommand extends Command {
	ShutdownCommand() { nArgs = 0; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) {
	    shutdownServer();
	}
    }
	    
    private static class SaveCommand extends Command {
	SaveCommand(int type) { nArgs = 0; commandType = type; }

	int commandType;

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    try {
		Tardis.save(tardisFile);

		if (commandType == 1)
		    printStatus(nbc, "Background saving started");
		else if (commandType == 2)
		    printStatus(nbc, "Background append only file rewriting started");
		else
		    printStatus(nbc);
	    } catch(Exception e) {
		e.printStackTrace(System.out);
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class LastSaveCommand extends Command {
	LastSaveCommand() { nArgs = 0; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    printInteger(nbc, tardisFile.lastModified());
	}
    }

    private static class AuthCommand extends Command {
	AuthCommand() { nArgs = 1; }
    }

    private static class SetCommand extends Command {
	SetCommand() { nArgs = 2; } //stringArg = true; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];
	    String value = args[1];

	    tardis.set(key, value);
	    printStatus(nbc);
	}
    }

    //
    // STRING COMMANDS
    //

    private static class GetCommand extends Command {
	GetCommand() { nArgs = 1; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];

	    try {
		String value = tardis.get(key);
		printResult(nbc, value);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class GetSetCommand extends Command {
	GetSetCommand() { nArgs = 2; stringArg = true; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];
	    String value = args[1];

	    try {
		String result = tardis.getset(key, value);
		printResult(nbc, result);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class MgetCommand extends Command {
	MgetCommand() { nArgs = VARARGS; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    List<String> values = tardis.mget(args);
	    printList(nbc, values);
	}
    }

    private static class SetnxCommand extends Command {
	SetnxCommand() { nArgs = 2; stringArg = true; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];
	    String value = args[1];

	    boolean result = tardis.setnx(key, value);
	    printInteger(nbc, result);
	}
    }

    private static class MsetCommand extends Command {
	MsetCommand() { nArgs = VARARGS; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    if (args.length % 2 != 0)
	        printError(nbc, "wrong number of arguments");
	    else {
	        tardis.mset(args);
	        printStatus(nbc);
	    }
	}
    }

    private static class MsetnxCommand extends Command {
	MsetnxCommand() { nArgs = VARARGS; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    if (args.length % 2 != 0)
	        printError(nbc, "wrong number of arguments");
	    else {
		boolean result = tardis.msetnx(args);
		printInteger(nbc, result);
	    }
	}
    }

    private static class AppendCommand extends Command {
	AppendCommand() { nArgs = 2; stringArg = true; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];
	    String value = args[1];

	    int result = tardis.append(key, value);
	    printInteger(nbc, result);
	}
    }


    private static class IncrCommand extends Command {
	IncrCommand() { nArgs = 1; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];

	    try {
		long value = tardis.incr(key);
		printInteger(nbc, value);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class IncrbyCommand extends Command {
	IncrbyCommand() { nArgs = 2; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];
	    long step = parseLong(args[1]);

	    try {
		long value = tardis.incrby(key, step);
		printInteger(nbc, value);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class DecrCommand extends Command {
	DecrCommand() { nArgs = 1; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];

	    try {
		long value = tardis.decr(key);
		printInteger(nbc, value);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class DecrbyCommand extends Command {
	DecrbyCommand() { nArgs = 2; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];
	    long step = parseLong(args[1]);

	    try {
		long value = tardis.decrby(key, step);
		printInteger(nbc, value);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class ExistsCommand extends Command {
	ExistsCommand() { nArgs = 1; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];

	    boolean result = tardis.exists(key);
	    printInteger(nbc, result);
	}
    }

    private static class DelCommand extends Command {
	DelCommand() { nArgs = VARARGS; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    int result = tardis.del(args);
	    printInteger(nbc, result);
	}
    }

    private static class TypeCommand extends Command {
	TypeCommand() { nArgs = 1; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];

	    String result = tardis.type(key);
	    printResult(nbc, result);
	}
    }

    //
    // KEY SPACE COMMANDS
    //

    private static class KeysCommand extends Command {
	KeysCommand() { nArgs = 1; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String pattern = args[0];

	    String result = tardis.keys(pattern);
	    printResult(nbc, result);
	}
    }

    private static class RandomkeyCommand extends Command {
	RandomkeyCommand() { nArgs = 0; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String result = tardis.randomkey();
	    printResult(nbc, result);
	}
    }

    private static class RenameCommand extends Command {
	RenameCommand() { nArgs = 2; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String oldname = args[0];
	    String newname = args[1];

	    try {
		tardis.rename(oldname, newname);
		printStatus(nbc);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class RenamenxCommand extends Command {
	RenamenxCommand() { nArgs = 2; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String oldname = args[0];
	    String newname = args[1];

	    try {
		boolean result = tardis.renamenx(oldname, newname);
		printInteger(nbc, result);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class DbsizeCommand extends Command {
	DbsizeCommand() { nArgs = 0; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    long result = tardis.dbsize();
	    printInteger(nbc, result);
	}
    }

    private static class ExpireCommand extends Command {
	ExpireCommand() { nArgs = 2; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];
	    long time = parseLong(args[1]) * 1000;

	    boolean result = tardis.expireat(key, System.currentTimeMillis()+time);
	    printInteger(nbc, result);
	}
    }

    private static class ExpireatCommand extends Command {
	ExpireatCommand() { nArgs = 2; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];
	    long time = parseLong(args[1]) * 1000;

	    boolean result = tardis.expireat(key, time);
	    printInteger(nbc, result);
	}
    }

    private static class TtlCommand extends Command {
	TtlCommand() { nArgs = 1; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];

	    long result = tardis.ttl(key);
	    if (result > 0)
		    result = (result+499)/1000;

	    printInteger(nbc, result);
	}
    }

    //
    // LIST COMMANDS
    //

    private static class RpushCommand extends Command {
	RpushCommand() { nArgs = 2; stringArg = true; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];
	    String value = args[1];

	    try {
		int result = tardis.rpush(key, value);
		printInteger(nbc, result);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class LpushCommand extends Command {
	LpushCommand() { nArgs = 2; stringArg = true; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];
	    String value = args[1];

	    try {
		int result = tardis.lpush(key, value);
		printInteger(nbc, result);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class LlenCommand extends Command {
	LlenCommand() { nArgs = 1; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];

	    try {
		int result = tardis.llen(key);
		printInteger(nbc, result);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class LrangeCommand extends Command {
	LrangeCommand() { nArgs = 3; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
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
    }

    private static class LtrimCommand extends Command {
	LtrimCommand() { nArgs = 3; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
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
    }

    private static class LindexCommand extends Command {
	LindexCommand() { nArgs = 2; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];
	    int index = parseInteger(args[1]);

	    try {
		String result = tardis.lindex(key, index);
		printResult(nbc, result);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class LsetCommand extends Command {
	LsetCommand() { nArgs = 3; stringArg = true; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];
	    int index = parseInteger(args[1]);
	    String value = args[2];

	    try {
		tardis.lset(key, index, value);
		printStatus(nbc);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class LremCommand extends Command {
	LremCommand() { nArgs = 3; stringArg = true; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];
	    int count = parseInteger(args[1]);
	    String value = args[2];

	    try {
		int result = tardis.lrem(key, count, value);
		printInteger(nbc, result);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class LpopCommand extends Command {
	LpopCommand() { nArgs = 1; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];

	    try {
		String result = tardis.lpop(key);
		printResult(nbc, result);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class RpopCommand extends Command {
	RpopCommand() { nArgs = 1; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];

	    try {
		String result = tardis.rpop(key);
		printResult(nbc, result);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class RpoplpushCommand extends Command {
	RpoplpushCommand() { nArgs = 2; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String src = args[0];
	    String dest = args[1];

	    try {
		String result = tardis.rpoplpush(src, dest);
		printResult(nbc, result);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    //
    // SETS COMMANDS
    //

    private static class SaddCommand extends Command {
	SaddCommand() { nArgs = 2; stringArg = true; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];
	    String member = args[1];

	    try {
		boolean result = tardis.sadd(key, member);
		printInteger(nbc, result);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class SremCommand extends Command {
	SremCommand() { nArgs = 2; stringArg = true; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];
	    String member = args[1];

	    try {
		boolean result = tardis.srem(key, member);
		printInteger(nbc, result);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class SpopCommand extends Command {
	SpopCommand() { nArgs = 1; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];

	    try {
		String result = tardis.spop(key);
		printResult(nbc, result);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class SmoveCommand extends Command {
	SmoveCommand() { nArgs = 3; stringArg = true; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String src = args[0];
	    String dest = args[1];
	    String member = args[2];

	    try {
		boolean result = tardis.smove(src, dest, member);
		printInteger(nbc, result);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class ScardCommand extends Command {
	ScardCommand() { nArgs = 1; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];

	    try {
		int result = tardis.scard(key);
		printInteger(nbc, result);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class SismemberCommand extends Command {
	SismemberCommand() { nArgs = 2; stringArg = true; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];
	    String member = args[1];

	    try {
		boolean result = tardis.sismember(key, member);
		printInteger(nbc, result);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class SmembersCommand extends Command {
	SmembersCommand() { nArgs = VARARGS; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    try {
		List<String> values = tardis.sinter(args);
		printList(nbc, values);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class SrandmemberCommand extends Command {
	SrandmemberCommand() { nArgs = 1; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];

	    try {
		String result = tardis.srandmember(key);
		printResult(nbc, result);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class SinterCommand extends Command {
	SinterCommand() { nArgs = VARARGS; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    try {
		List<String> values = tardis.sinter(args);;
		printList(nbc, values);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class SinterstoreCommand extends Command {
	SinterstoreCommand() { nArgs = VARARGS; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    try {
		int result = tardis.sinterstore(args);;
		printInteger(nbc, result);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class SunionCommand extends Command {
	SunionCommand() { nArgs = VARARGS; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    try {
		List<String> values = tardis.sunion(args);;
		printList(nbc, values);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class SunionstoreCommand extends Command {
	SunionstoreCommand() { nArgs = VARARGS; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    try {
		int result = tardis.sunionstore(args);;
		printInteger(nbc, result);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class SdiffCommand extends Command {
	SdiffCommand() { nArgs = VARARGS; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    try {
		List<String> values = tardis.sdiff(args);;
		printList(nbc, values);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class SdiffstoreCommand extends Command {
	SdiffstoreCommand() { nArgs = VARARGS; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    try {
		int result = tardis.sdiffstore(args);;
		printInteger(nbc, result);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    //
    // ZSETS COMMANDS
    //

    private static class ZaddCommand extends Command {
	ZaddCommand() { nArgs = 3; stringArg = true; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];
	    String score = args[1];
	    String member = args[2];

	    try {
		boolean result = tardis.zadd(key, score, member);
		printInteger(nbc, result);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class ZincrbyCommand extends Command {
	ZincrbyCommand() { nArgs = 3; stringArg = true; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];
	    String score = args[1];
	    String member = args[2];

	    try {
		double result = tardis.zincrby(key, score, member);
		printDouble(nbc, result);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class ZremCommand extends Command {
	ZremCommand() { nArgs = 2; stringArg = true; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];
	    String member = args[1];

	    try {
		boolean result = tardis.zrem(key, member);
		printInteger(nbc, result);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class ZrangeCommand extends Command {
	ZrangeCommand() { nArgs = VARARGS; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
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
    }

    private static class ZrevrangeCommand extends Command {
	ZrevrangeCommand() { nArgs = VARARGS; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
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
    }

    private static class ZrangebyscoreCommand extends Command {
	ZrangebyscoreCommand() { nArgs = VARARGS; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    try {
	       String key = args[0];
	       String min = args[1];
	       String max = args[2];
	       int offset = 0;
	       int count = Integer.MAX_VALUE;
	       boolean withscores = false;

		for (int i=3; i < args.length; i++) {
		    String arg = args[i];

		    if ("LIMIT".equalsIgnoreCase(arg)) {
			offset = parseInteger(args[++i]);
			count = parseInteger(args[++i]);
		    } else if ("WITHSCORES".equalsIgnoreCase(arg)) {
			withscores = true;
		    } else
			throw new UnsupportedOperationException(Tardis.SYNTAX);
		}
	    
		List<String> values = tardis.zrangebyscore(key, min, max, offset, count, withscores);
		printList(nbc, values);
	    } catch(ArrayIndexOutOfBoundsException e) {
		printError(nbc, Tardis.SYNTAX);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class ZremrangebyscoreCommand extends Command {
	ZremrangebyscoreCommand() { nArgs = 3; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
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
    }

    private static class Zremrangebyrank extends Command {
	Zremrangebyrank() { nArgs = 3; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];
	    int start = parseInteger(args[1]);
	    int end = parseInteger(args[2]);

	    try {
		int result = tardis.zremrangebyrank(key, start, end);
		printInteger(nbc, result);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class ZcardCommand extends Command {
	ZcardCommand() { nArgs = 1; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];

	    try {
		int result = tardis.zcard(key);
		printInteger(nbc, result);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class ZscoreCommand extends Command {
	ZscoreCommand() { nArgs = 2; stringArg = true; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];
	    String member = args[1];

	    try {
		String result = tardis.zscore(key, member);
		printResult(nbc, result);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class ZrankCommand extends Command {
	ZrankCommand() { nArgs = 2; stringArg = true; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];
	    String member = args[1];

	    try {
		int result = tardis.zrank(key, member);
		printResult(nbc, result);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class ZrevrankCommand extends Command {
	ZrevrankCommand() { nArgs = 2; stringArg = true; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];
	    String member = args[1];

	    try {
		int result = tardis.zrevrank(key, member);
		printResult(nbc, result);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class ZcountCommand extends Command {
	ZcountCommand() { nArgs = 3; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    try {
		String key = args[0];
		String min = args[1];
		String max = args[2];

		int result = tardis.zcount(key, min, max);

		printInteger(nbc, result);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    //
    // SORT
    //

    private static class SortCommand extends Command {
	SortCommand() { nArgs = VARARGS; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
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
    }

    //
    // MULTIPLE DB COMMANDS
    //
    
    private static class SelectCommand extends Command {
	private int selection = -1;

	SelectCommand() { nArgs = 1; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    int index = parseInteger(args[0]);

	    try {
		Tardis.select(index);
		selection = index;
		printStatus(nbc);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}

	public int getSelected() {
		return selection;
	}
    }

    private static class MoveCommand extends Command {
	MoveCommand() { nArgs = 2; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String key = args[0];
	    int index = parseInteger(args[1]);

	    try {
		boolean result = tardis.move(key, index);
		printInteger(nbc, result);
	    } catch(Exception e) {
		printError(nbc, e.getMessage());
	    }
	}
    }

    private static class FlushdbCommand extends Command {
	FlushdbCommand() { nArgs = 0; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    tardis.flushdb();
	    printStatus(nbc);
	}
    }

    private static class FlushallCommand extends Command {
	FlushallCommand() { nArgs = 0; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    Tardis.flushall();
	    printStatus(nbc);
	}
    }

    private static class InfoCommand extends Command {
	InfoCommand() { nArgs = 0; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    printResult(nbc, "version:"
		    + Tardis.V_MAJOR
		    + "."
		    + Tardis.V_MINOR);
	}
    }

    private static class DebugCommand extends Command {
	DebugCommand() { nArgs = 1; }

	public void run(Tardis tardis, String args[], INonBlockingConnection nbc) throws IOException {
	    String cmd = args[0];
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
    }

    private static class Request {
	public Command cmd;
	public String args[];

	Request(Command cmd, String args[]) {
	    this.cmd = cmd;
	    this.args = args;
	}

	public int run(int dbIndex, INonBlockingConnection nbc) throws IOException {
	    Tardis tardis = Tardis.select(dbIndex);
	    cmd.run(tardis, args, nbc);

	    if (cmd instanceof SelectCommand)
		dbIndex = ((SelectCommand)cmd).getSelected();

	    return dbIndex;
	}
    }

    public static void main(String[] args)
    {
	for (String arg : args) {
		if (arg.equals("--debug"))
			DEBUG = true;

		else if (arg.startsWith("--password="))
			password = arg.substring(11);

		else {
			System.out.println("invalid argument: " + arg);
			System.exit(1);
		}
	}

	if (DEBUG)
		System.out.println("DEBUG enabled");

	if (password != null)
		System.out.println("AUTHENTICATION required");

	commands.put("",	    new NopCommand());
    	commands.put("ping",        new PingCommand());
    	commands.put("quit",        new QuitCommand());
    	commands.put("shutdown",    new ShutdownCommand());
	commands.put("save",        new SaveCommand(0));
	commands.put("bgsave",      new SaveCommand(1));
	commands.put("bgrewriteaof",new SaveCommand(2));
	commands.put("lastsave",    new LastSaveCommand());
	commands.put("auth",        new AuthCommand());

    	commands.put("set",         new SetCommand());
    	commands.put("get",         new GetCommand());
    	commands.put("getset",      new GetSetCommand());
    	commands.put("mget",        new MgetCommand());
    	commands.put("setnx",       new SetnxCommand());
    	commands.put("mset",        new MsetCommand());
    	commands.put("msetnx",      new MsetnxCommand());
    	commands.put("append",      new AppendCommand());
    	commands.put("incr",        new IncrCommand());
    	commands.put("incrby",      new IncrbyCommand());
    	commands.put("decr",        new DecrCommand());
    	commands.put("decrby",      new DecrbyCommand());
    	commands.put("exists",      new ExistsCommand());
    	commands.put("del",         new DelCommand());
    	commands.put("type",        new TypeCommand());

    	commands.put("keys",        new KeysCommand());
    	commands.put("randomkey",   new RandomkeyCommand());
    	commands.put("rename",      new RenameCommand());
    	commands.put("renamenx",    new RenamenxCommand());
    	commands.put("dbsize",      new DbsizeCommand());
    	commands.put("expire",      new ExpireCommand());
    	commands.put("expireat",    new ExpireatCommand());
    	commands.put("ttl",         new TtlCommand());

	commands.put("rpush",       new RpushCommand());
	commands.put("lpush",       new LpushCommand());
	commands.put("llen",        new LlenCommand());
	commands.put("lrange",      new LrangeCommand());
	commands.put("ltrim",       new LtrimCommand());
	commands.put("lindex",      new LindexCommand());
	commands.put("lset",        new LsetCommand());
	commands.put("lrem",        new LremCommand());
	commands.put("lpop",        new LpopCommand());
	commands.put("rpop",        new RpopCommand());
	commands.put("rpoplpush",   new RpoplpushCommand());

	commands.put("sadd",        new SaddCommand());
	commands.put("srem",        new SremCommand());
	commands.put("spop",        new SpopCommand());
	commands.put("smove",       new SmoveCommand());
        commands.put("scard",       new ScardCommand());
	commands.put("sismember",   new SismemberCommand());
	commands.put("smembers",    new SmembersCommand());
	commands.put("srandmember", new SrandmemberCommand());
	commands.put("sinter",      new SinterCommand());
	commands.put("sinterstore", new SinterstoreCommand());
	commands.put("sunion",      new SunionCommand());
	commands.put("sunionstore", new SunionstoreCommand());
	commands.put("sdiff",       new SdiffCommand());
	commands.put("sdiffstore",  new SdiffstoreCommand());

	commands.put("zadd",        new ZaddCommand());
	commands.put("zrem",        new ZremCommand());
	commands.put("zincrby",     new ZincrbyCommand());
	commands.put("zrank",       new ZrankCommand());
	commands.put("zrevrank",    new ZrevrankCommand());
	commands.put("zrange",      new ZrangeCommand());
	commands.put("zrevrange",   new ZrevrangeCommand());
	commands.put("zrangebyscore",    new ZrangebyscoreCommand());
	commands.put("zremrangebyrank",  new Zremrangebyrank());
	commands.put("zremrangebyscore", new ZremrangebyscoreCommand());
	commands.put("zcard",       new ZcardCommand());
	commands.put("zscore",      new ZscoreCommand());
	commands.put("zcount",      new ZcountCommand());

	commands.put("sort",        new SortCommand());

	commands.put("select",      new SelectCommand());
	commands.put("move",        new MoveCommand());
	commands.put("flushdb",     new FlushdbCommand());
	commands.put("flushall",    new FlushallCommand());

	commands.put("info",        new InfoCommand());
	commands.put("debug",       new DebugCommand());

	commands.put("multi",	    new NopCommand()); // executed inline
	commands.put("exec",	    new NopCommand()); // executed inline
	commands.put("discard",	    new NopCommand()); // executed inline

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
            System.out.println("exception " + ex);
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
	    String cmdname = "";

	    if (data.startsWith("*")) {
		List<String> list = parseList(nbc, data);
		if (list !=  null) {
		    cmdname = list.remove(0).toLowerCase();
		    args = list.toArray(new String[0]);
		}
	    } else {
	        args = data.trim().split(" ", 2);
	        cmdname = args[0].toLowerCase();

	        if (args.length > 1)
	    	    args = args[1].split(" ");
	        else
	            args = new String[0];
	    }

	    if (DEBUG) {
		System.out.println("     " + cmdname + " " + args);
	    }

	    Command cmd = commands.get(cmdname);
	    if (cmd != null) {
		if (cmd.stringArg) {
		    String arg = args[args.length-1];
		    args[args.length-1] = parseString(nbc, arg);
		}
	    }

	    if (DEBUG) {
		System.out.println("cmd: " + cmdname 
			+ ", info: " + cmd);
	    }

	    if (cmd == null)
		printError(nbc, "unknown command");

	    else if (cmd.nArgs != VARARGS && cmd.nArgs != args.length) {
	        if (DEBUG) {
		    System.out.println("command: " + cmdname);
		    System.out.println("args: " + args.length);
		}

		printError(nbc, "wrong number of arguments for '" 
		    + cmdname + "' command");
	    }

	    else if (password != null && !authenticated && !cmdname.equals("auth")) {
System.out.println("password: " + password);
System.out.println("authenticated: " + authenticated);
System.out.println("cmdname: " + cmdname);
		printError(nbc, "operation not permitted");
	    }

	    else if (cmdname.equals("auth")) {
		if (password == null || password.equals(args[0])) {
		    authenticated = true;
		    printStatus(nbc);
		} else {
		    authenticated = false;
		    printError(nbc, "invalid password");
		}
	    }

	    else if (cmdname.equals("multi")) {
	        multi = true;
	        printStatus(nbc);
            } 

	    else if (cmdname.equals("exec")) {
		if (!multi)
			printError(nbc, "EXEC without MULTI");
		else {
			multi = false;
		
			printList(nbc, requests.size());
			while (! requests.isEmpty()) {
				Request r = requests.remove(0);
				selected = r.run(selected, nbc);
			}

		}
	    }

	    else if (cmdname.equals("discard")) {
		if (!multi)
			printError(nbc, "DISCARD without MULTI");
		else {
			multi = false;
			requests.clear();
			printStatus(nbc);
		}
	    }

	    else if (multi) {
		requests.add(new Request(cmd, args));
		printStatus(nbc, "QUEUED");
            }

	    else {
		Tardis tardis = Tardis.select(selected);
		cmd.run(tardis, args, nbc);

		if (cmd instanceof SelectCommand)
		    selected = ((SelectCommand)cmd).getSelected();
            }

if (DEBUG) System.out.println("normal termination");
	    nbc.removeReadMark();
	} catch(UnsupportedOperationException e1) {
if (DEBUG) System.out.println("unsupported operation");
	    printError(nbc, e1.getMessage());
	} catch(BufferUnderflowException e2) {
if (DEBUG) System.out.println("underflow");
	    nbc.resetToReadMark();
	} catch(ClosedChannelException e3) {
	    System.out.println("connection closed");
	} catch(Exception e4) {
	    e4.printStackTrace(System.out);
	}
	 
	return true;
    }

    public static int parseInteger(String v)
    {
	try {
	    return Integer.parseInt(v);
	} catch(Exception e) {
	    return 0;
	}
    }

    public static long parseLong(String v)
    {
	try {
	    return Long.parseLong(v);
	} catch(Exception e) {
	    return 0;
	}
    }

    public static String parseString(INonBlockingConnection nbc, String v)
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

    public static List<String> parseList(INonBlockingConnection nbc, String v)
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

	// this only writes the list header (number of elements in the list)
    public static void printList(INonBlockingConnection nbc, int n)
	throws IOException
    {
	nbc.write("*" + n + EOL);
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
