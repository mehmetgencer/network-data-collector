#!/usr/bin/python3
"""
This is a program for collecting datasets from a news (NNTP) server.
The program can be interrupted, and restarted at any time.
"""
import sys, getopt, re,  os, time, calendar, traceback, json, string, nntplib, os.path, pprint, datetime, time
mypath=os.path.dirname(sys.argv[0])
sys.path.append(mypath+os.path.sep+"yserial")
import y_serial_v060py3 as yserial
from xml.etree.ElementTree import TreeBuilder, ElementTree
    
DEBUG=1
PERSISTENCE=os.path.expanduser("~/network-data-collector")
global storage

def debug(*args,level=1,**kw):
    """
    Print all args to stderr, if level is less than or equal to global DEBUG level.
    "progress" option in kw causes a carriage return ( \r ) instead of 
    a newline at the end
    """
    if level<=DEBUG:
        if "progress" in kw:progress=kw["progress"]
        else:progress=False
        if progress:sys.stderr.write("\r")
        for a in args:sys.stderr.write(str(a)+" ")
        if not progress:sys.stderr.write("\n")

try:
    from dateutil import parser
except:
    debug("You do not have datautil installed! Aborting")
    sys.exit(1)
    
def parseEmailAddress(addr):
    """
    Parses an email such as "John Smith <jsmith@example.com>" and 
    returns the jsmith@example.com part
    """
    debug("parsing e-mail",addr,level=3)
    aman=addr
    #for p,r in [("<"," <"),(">","> "),("("," ("),(")",") ")]:
    #    aman=aman.replace(p,r)
    try:
        r=re.compile("^(.*?)[\\s<]*(\\S+@\\S+)[>\\s]*(.*?)$")
        n1,em,n2=r.findall(aman)[0]
        n=n1.strip()+" "+n2.strip()
        n=n.strip().lower()
        for p in ["(",")","\"","'"]:n=n.replace(p,"")
        em=em.replace(">","").replace("_REMOVE","").replace("REMOVE","").replace("/","")
        x1,x2=em.split("@")
    except:
        x1,x2,n=aman.strip(),"",""
    email=x1+"@"+x2
    debug("Parsed email",addr,"to mail:",email,", name:",n,level=3)
    return email

def formatTstamp(ts):
    return str(ts)
    
class NNTPDataset(dict):
    """
    Represents a dataset collected from a news (NNTP) server, 
    like news.gmane.org
    It is persisted in a yserial database in global 'storage'
    """
    @classmethod
    def restore(cls):
        dataset= storage.select("dataset","dataset")
        #pprint.pprint(dataset)
        return dataset
    
    def __init__(self,**kw):
        dict.__init__(kw)
        self.actors=None
    
    def persist(self):
        storage.insert(self,"dataset","dataset")
    
    def setup(self):
        """
        (Re-)setup the information necessary prior to connecting to server
        (such as server host, port, username and password) and 
        retrieving selected mailgroups (i.e. asks which mailgroups to
        retrieve).
        """
        def askSetting(s,default=None,convert=str):
            if s in self and self[s]:default=self[s]
            tmp=input(s+" (default: %s) :"%default)
            if tmp:
                self[s]=convert(tmp)
            else:
                self[s]=convert(default)
        askSetting("server",default="news.gmane.org")
        askSetting("port",default=119,convert=int)
        askSetting("username",default="")
        askSetting("password",default="")
        self.persist()

        groups=self.get("groups",{})
        if groups:
            print("Following groups were selected for download from this server:")
            for gname,g in groups.items():
                print(gname," (number of messages: %d)"%(g["last"]-g["first"]+1))
        else:
            print("No groups were selected for download yet")
            
        tmp=input("Will get a list of mailgroups at server. Press ENTER to continue, or enter 'no' to skip:")
        if not tmp:
            resp,glist=self.getServer().list()
            ginfo={}
            for g,last,first,flag in glist:
                gname=g.decode("ascii")
                ilast=int(last.decode("ascii"))
                ifirst=int(first.decode("ascii"))
                print(gname, "(number of messages: %d)"%(ilast-ifirst+1))
                ginfo[gname]={"last":ilast,"first":ifirst}
            
        print("Enter the group names you want to collect, finish with an empty line")
        while True:
            gname=input("Enter group name:").strip()
            if not gname:break
            if not gname in groups:
                groups[gname]=ginfo[gname]
                self.setGroupPointer(gname, ginfo[gname]["first"])
        self["groups"]=groups
        self.persist()
        print("Setup is completed.")

    def getMinMaxTstamp(self):
        mint=None
        maxt=None
        tstamps=[]
        c=0
        for x,(ign,mid,info) in storage.selectdic("*","emails").items():
            c+=1
            debug("Checking timestamps %d"%c,level=2,progress=True)
            tstamps.append(parser.parse(info["tstamp"]))
        return min(tstamps),max(tstamps)
    def summarize(self):
        """Print a summary of this dataset"""
        for x in self:
            print(x,end=" : ")
            if x in ["password"]:
                print("*HIDDEN*")
            elif x=="groups":
                print(len(self[x])," groups selected")
            else:
                print(self[x]) 
        if "groups" in self:
            print("GROUPS:")
            for gname,g in self["groups"].items():
                print("  ",gname)
                
    def getServer(self):
        debug("Connecting to NNTP server: %s:%d"%(self["server"],self["port"]))
        self.server=nntplib.NNTP(self["server"], self["port"], self["username"] , self["password"],1)
        return self.server
    
    def getGroupPointer(self,gname):
        return storage.select(gname,"grouppointers")
    
    def setGroupPointer(self,gname,pointer):
        storage.insert(pointer,gname,"grouppointers")

    def incrementGroupPointer(self,gname):
        self.setGroupPointer(gname,self.getGroupPointer(gname)+1)
        debug("Downloaded",gname,self.getGroupPointer(gname),progress=True)
        
    def hasMoreInGroup(self,gname):
        if self.getGroupPointer(gname)>self["groups"][gname]["last"]:
            return False
        return True
    
    def addEmail(self,uniqueID,**kw):
        storage.insert(kw,uniqueID,"emails")
        debug("Added ",uniqueID,kw,level=2)
        
    def download(self):
        server=self.getServer()
        patterns={"date":"NNTP-Posting-Date:","sender":"From:","refs":"References:"}
        for gname,g in self["groups"].items():
            server.group(gname)
            while self.hasMoreInGroup(gname):
                i=self.getGroupPointer(gname)
                err=False
                try:
                    response, number, ID, headerlist=server.head(str(i))
                    ID=ID.decode("ascii")
                    uniqueID=gname+"-"+ID #to prevent msg ID conflicts across mailgroups
                    vals={}
                    for h in headerlist:
                        h=h.decode("ascii")
                        for pk in patterns:
                            p=patterns[pk]
                            if h.find(p)==0:vals[pk]=h[len(p):].strip()
                    refs=vals.get("refs","").split()
                    senderemail=parseEmailAddress(vals["sender"])
                    self.addEmail(uniqueID,msgid=ID,gname=gname,senderemail=senderemail,tstamp=vals["date"],references=refs)
                except nntplib.NNTPTemporaryError as e:
                    debug("NNTP error:",gname,i,e,level=1)
                    err=True
                except UnicodeDecodeError as e:
                    debug(vals,level=3)
                    debug("Unicode error:",gname,i,e,level=1)
                    err=True
                except KeyboardInterrupt:
                    debug("INTERRUPTED",level=1)
                    return
                self.incrementGroupPointer(gname)
    def dump(self,targetfp):
        for x,(ign,mid,info) in storage.selectdic("*","emails").items():
            gname=info["gname"]
            msgid=info["msgid"]
            senderemail=info["senderemail"]
            tstamp=info["tstamp"]
            if info["references"]:
                lastref=info["references"][-1]
                print("Searching lastref",lastref)
                ref=storage.select(gname+"-"+lastref,"emails")
                if not ref:
                    print("Reference not found")
                    continue
                recipient=ref["senderemail"]
            else:
                recipient=None
            print(gname, msgid, senderemail, recipient, tstamp)
        #msgid=ID,gname=gname,senderemail=senderemail,tstamp=vals["date"],references=refs
        #targetfp.write("Not implemented yet\n")
    def dumpcco(self,targetfp):
        retval=[]
        for x,(ign,mid,info) in storage.selectdic("*","emails").items():
            gname=info["gname"]
            msgid=info["msgid"]
            senderemail=info["senderemail"]
            tstamp=info["tstamp"]
            ts=parser.parse(tstamp)
            refs=[]
            if info["references"]:refs=info["references"]
            m={"id":msgid,"from":senderemail, "timestamp":calendar.timegm(ts.timetuple()), "references":refs}
            retval.append(m)
        targetfp.write(json.dumps(retval))

    def DELETEdumpcco(self,targetfp):
        targetfp.write("[\n")
        j=0
        for x,(ign,mid,info) in storage.selectdic("*","emails").items():
            if j>0:targetfp.write(",\n")
            gname=info["gname"]
            msgid=info["msgid"]
            senderemail=info["senderemail"]
            tstamp=info["tstamp"]
            ts=parser.parse(tstamp)
            refs="[]"
            if info["references"]:
                refs="["
                i=0
                for s in info["references"]:
                    if i>0:refs+=", "
                    refs+="\"%s\""%s
                    i+=1
                refs+="]"
            targetfp.write("""{\n  "id": "%s",\n  "from": "%s",\n  "timestamp": %s,\n  "references": %s\n}\n"""%(msgid,senderemail,calendar.timegm(ts.timetuple()),refs))
            j+=1
        targetfp.write("]")
    def ensureDumpBase(self):
        try:
            self.actors
            return #already done
        except:
            debug("Building data for dumping out")
        self.mint,self.maxt=self.getMinMaxTstamp()
        actors={}
        c=0
        for x,(ign,mid,info) in storage.selectdic("*","emails").items():
            c+=1
            debug("Processing actor %d"%c,level=2,progress=True)
            gname=info["gname"]
            msgid=info["msgid"]
            senderemail=info["senderemail"]
            if not senderemail in actors:actors[senderemail]=[]
            tstamp=info["tstamp"]
            actors[senderemail].append(parser.parse(tstamp))
            if info["references"]:
                lastref=info["references"][-1]
                ref=storage.select(gname+"-"+lastref,"emails")
                if not ref:
                    debug("Reference not found",level=2)
                    continue
                recipient=ref["senderemail"]
                if not recipient in actors:actors[recipient]=[]
                actors[recipient].append(parser.parse(tstamp))
            else:
                recipient=None
        actorids={}
        id=0
        for i,v in actors.items():
            actorids[i]=id
            id+=1        
        self.actors=actors
        self.actorids=actorids
        edges=[]
        id=0
        c=0
        for x,(ign,mid,info) in storage.selectdic("*","emails").items():
            c+=1
            debug("Processing relation %d"%c,level=2,progress=True)
            gname=info["gname"]
            msgid=info["msgid"]
            senderemail=info["senderemail"]
            tstamp=info["tstamp"]
            pts=parser.parse(tstamp)
            if info["references"]:
                lastref=info["references"][-1] #the last reference is the immediate parent of the message reply
                ref=storage.select(gname+"-"+lastref,"emails")
                if not ref:
                    debug("Reference not found",level=2)
                    continue
                recipient=ref["senderemail"]
                if senderemail in actorids and recipient in actorids:
                    edge=(id,actorids[senderemail],actorids[recipient],formatTstamp(pts))
                    edges.append(edge)
                    id+=1
                else:
                    debug("Actor id not found",level=2)
            else:
                recipient=None
        self.edges=edges
    def nodesIterator(self):
        """Yields (id, email, minTime, maxTime) for each actor"""
        for email,id in self.actorids.items():
            yield (id,email,min(self.actors[email]),max(self.actors[email]))
    def edgesIterator(self):
        for e in self.edges:
            yield e
    def dumpGEXF(self,targetfp,projectName):
        self.ensureDumpBase()
        tb=TreeBuilder()
        tb.start("gexf",{"xmlns":"http://www.gexf.net/1.2draft","version":"1.2"})
        tb.start("meta",{"lastmodifieddate":str(datetime.datetime.now())})
        tb.start("creator",{})
        tb.data("LAVI Collector")
        tb.end("creator")
        tb.start("description",{})
        tb.data(projectName)
        tb.end("description")
        tb.end("meta")
        tb.start("graph",{"mode":"dynamic","start":formatTstamp(self.mint),"end":formatTstamp(self.maxt)})
        tb.start("nodes",{})
        for id,email,mint,maxt in self.nodesIterator():
            tb.start("node",{"id":id,"label":email,"start":formatTstamp(mint),"end":formatTstamp(maxt)})
            tb.end("node")
        tb.end("nodes")
        tb.start("edges",{})
        for id,sender,rec,tstamp in self.edgesIterator():
            tb.start("edge",{"id":id, "source":sender,"target":rec,"start":formatTstamp(tstamp),"end":formatTstamp(tstamp)})
            tb.end("edge")
        tb.end("edges")
        tb.end("graph")
        tb.end("gexf")
        et=ElementTree(tb.close())
        et.write(targetfp, encoding="UTF-8")
    def dumpGEXFDELETE(self,targetfp,projectName):
        mint,maxt=self.getMinMaxTstamp()
        targetfp.write("""
<?xml version="1.0" encoding="UTF-8"?>
<gexf xmlns="http://www.gexf.net/1.2draft" version="1.2">
 <meta lastmodifieddate="2009-03-20">
        <creator>LAVI Collector</creator>
        <description>%s</description>
 </meta>
 <graph mode="dynamic" start="%s" end="%s">
        """%(projectName,formatTstamp(mint),formatTstamp(maxt)))
        actors={}
        for x,(ign,mid,info) in storage.selectdic("*","emails").items():
            gname=info["gname"]
            msgid=info["msgid"]
            senderemail=info["senderemail"]
            if not senderemail in actors:actors[senderemail]=[]
            tstamp=info["tstamp"]
            actors[senderemail].append(parser.parse(tstamp))
            if info["references"]:
                lastref=info["references"][-1]
                ref=storage.select(gname+"-"+lastref,"emails")
                if not ref:
                    debug("Reference not found",level=2)
                    continue
                recipient=ref["senderemail"]
                if not recipient in actors:actors[recipient]=[]
                actors[recipient].append(parser.parse(tstamp))
            else:
                recipient=None
        targetfp.write("  <nodes>\n")
        actorids={}
        id=0
        for i,v in actors.items():
            actorids[i]=id
            targetfp.write("""    <node id="%d" label="%s" start="%s" end="%s" />\n"""%(id,i,formatTstamp(min(v)),formatTstamp(max(v))))
            id+=1
        targetfp.write("  </nodes>\n")
        targetfp.write("  <edges>\n")
        id=0
        for x,(ign,mid,info) in storage.selectdic("*","emails").items():
            gname=info["gname"]
            msgid=info["msgid"]
            senderemail=info["senderemail"]
            tstamp=info["tstamp"]
            pts=parser.parse(tstamp)
            if info["references"]:
                lastref=info["references"][-1]
                ref=storage.select(gname+"-"+lastref,"emails")
                if not ref:
                    debug("Reference not found",level=2)
                    continue
                recipient=ref["senderemail"]
                targetfp.write("""    <edge id="%d" source="%d" target="%d" start="%s" end="%s"/>\n"""%(id,actorids[senderemail],actorids[recipient],formatTstamp(pts),formatTstamp(pts)))
                id+=1
            else:
                recipient=None
        targetfp.write("  </edges>\n")
        targetfp.write("""
 </graph>
</gexf>        
        """)
    def resetActors(self):
        storage.insert({},"_","actors")
        storage.delete("*","actors")
        self.actorid=0
    def ensureActor(self,email):
        if not storage.select(email,"actors"):
            self.actorid+=1
            #print("Adding actor:",email)
            storage.insert({"email":email,"iid":self.actorid},email,"actors")
    def getActorId(self,email):
        a=storage.select(email,"actors")
        return a["iid"]
    def getActId(self,msgid):
        #debug("Seeking act for msgid:"+msgid)
        a=storage.select(msgid,"acts")
        return a["iid"]
    def resetRelations(self):
        storage.insert({},"_","relations")
        storage.delete("*","relations")
        self.relationid=0
    def ensureEmail(self,msgid,sender=None,recipient=None,tstampstr=None):
        if not storage.select(msgid,"relations"):
            self.relationid+=1
            storage.insert({"iid":self.relationid,"sender":sender,"recipient":recipient,"tstampstr":tstampstr},msgid,"relations")
    def resetActs(self):
        storage.insert({},"_","acts")
        storage.delete("*","acts")
        self.actid=0
    def ensureAct(self,msgid,sender=None,reference=None,tstampstr=None,type=None):
        if not storage.select(msgid,"acts"):
            self.actid+=1
            #debug("Ensuring act (%d) for reference:"%self.actid+str(reference)+", and msgid:"+str(msgid))
            storage.insert({"iid":self.actid,"sender":sender,"reference":reference,"tstampstr":tstampstr,"type":type},msgid,"acts")
    def dumpGeneric(self,reset=False,output=True):
        try:
            actors=storage.select("*","actors")
        except:
            actors=[]
        if actors is None or reset:
            debug("Building actor list")
            self.resetActors()
            self.resetRelations()
            self.resetActs()
            c=0
            errs=0
            for uid, (ign,mid,info) in storage.selectdic("*","emails").items():
                c+=1
                debug("Processing email %d"%c,level=2,progress=True)
                gname=info["gname"]
                msgid=info["msgid"]
                senderemail=info["senderemail"]
                tstamp=info["tstamp"]
                #pts=parser.parse(tstamp)
                self.ensureActor(senderemail)
                if info["references"]:
                    lastref=info["references"][-1]
                    ref=storage.select(gname+"-"+lastref,"emails")
                    if not ref:
                        debug("Reference not found",level=2)
                        errs+=1
                        #TODO: Following is not really correct!
                        debug("PHONY 'CALL' ACT (insertin call instead of a reply since reply reference is not found)!")
                        self.ensureAct(msgid,sender=senderemail,tstampstr=tstamp,type="call")
                    else:
                        recipient=ref["senderemail"]
                        self.ensureActor(recipient)
                        self.ensureEmail(msgid,sender=senderemail,recipient=recipient,tstampstr=tstamp)
                        self.ensureAct(msgid,sender=senderemail,reference=lastref,tstampstr=tstamp,type="reply")
                else:
                    self.ensureAct(msgid,sender=senderemail,tstampstr=tstamp,type="call")
        if output:
            for i,(x,y,a) in storage.selectdic("*","actors").items():
                print("actor:",a["iid"])
        print("Number of reference errors: %d"%errs)
    def dumpFan(self,targetfp,projectName="noname",reset=False):
        if not reset:
            debug("Starting dump without reset")
        self.dumpGeneric(reset=reset,output=False)
        targetfp.write("""
using lavi
using sys
Network {
    name="%s"
    meta=["creator":"Lavi","description":"A simple static network"]
    actors=[
        """%projectName)
        for i,(x,y,a) in storage.selectdic("*","actors").items():
            targetfp.write("""Actor{id=%d; name="%s"},\n"""%(a["iid"],a["email"].replace('"','')))
        targetfp.write("""
        ]
        relations=[
        """)
        for i,(x,y,r) in storage.selectdic("*","relations").items():
            #print("RELATION:",r)
            pts=parser.parse(r["tstampstr"])
            srca=self.getActorId(r["sender"])
            targetfp.write("""Relation{actors=[%d,%d];src=%d;startTstamp=DateTime("%s Istanbul")},\n"""%(srca,self.getActorId(r["recipient"]),srca,pts.isoformat()))
            #targetfp.write("""Relation{actors=[%d,%d],start=%df},\n"""%(self.getActorId(r["sender"]),self.getActorId(r["recipient"]),time.mktime(pts.timetuple())))
        targetfp.write("""
        ]
       }
        """)
    def dumpLax(self,targetfp,projectName="noname",reset=False):
        if not reset:
            debug("Starting dump without reset")
        self.dumpGeneric(reset=reset,output=False)
        targetfp.write("""<?xml version="1.0" encoding="UTF-8"?>
<lax>
<meta>
 <name>%s</name>
</meta>  
<actors>
        """%projectName)
        for i,(x,y,a) in storage.selectdic("*","actors").items():
            targetfp.write("""<actor id='%d' name="%s"/>\n"""%(a["iid"],a["email"].replace('"','')))
        targetfp.write("""
</actors>
<actions>
        """)
        for i,(x,y,r) in storage.selectdic("*","acts").items():
            #print("RELATION:",r)
            pts=parser.parse(r["tstampstr"])
            srca=self.getActorId(r["sender"])
            if r["type"]=="call":
                targetfp.write("""<act type='%s' id='%d' src='%d' time='%d'/>\n"""%(r["type"],r["iid"],srca,time.mktime(pts.timetuple())))
            else:
                targetfp.write("""<act type='%s' id='%d' src='%d' reference='%d' time='%d'/>\n"""%(r["type"],r["iid"],srca,self.getActId(r["reference"]),time.mktime(pts.timetuple())))
            #targetfp.write("""Relation{actors=[%d,%d],start=%df},\n"""%(self.getActorId(r["sender"]),self.getActorId(r["recipient"]),time.mktime(pts.timetuple())))
        targetfp.write("""
</actions>
</lax>
        """)
def printHelp():
    debug("Usage:")
    debug("   %s [options] <project-name> [command [command-opts]]"%(sys.argv[0]))
    debug("")
    debug(" Options:")
    debug("   -v: verbose mode, print debug messages (can use multiple times)")
    debug(" Commands:")
    debug("   summarize : this is the default command, summarizes options")
    debug("   setup : asks questions to initialize what to collect and from where")
    debug("   collect : start or continue collecting data from where left")
    debug("   delete : remove this project from workspace")
    #debug("   dump <target>: dump collected data out. Target is a filename, or - for stdout.")
    #debug("   dumpcco <target>: dump collected data out in cco format. Target is a filename, or - for stdout.")
    debug("   dumpGEXF <target>: dump collected data out in Gephi GEXF format. Target is a filename, or - for stdout.")    
    debug("   dumpfan [-r] <target>: dump collected data out in Fantom language serialization format. Target is a filename, or - for stdout. -r prevents resetting of data processing")    
    debug("   dumplax [-r] <target>: dump collected data out in Lavi XML format serialization format. Target is a filename, or - for stdout. -r prevents resetting of data processing")    
if __name__=="__main__":
    opts, args = getopt.getopt(sys.argv[1:], "v", [])
    for o, a in opts:
        if o == "-v":
            DEBUG += 1
    if args:
        projectName=args.pop(0)
    else:
        print("You must give a project name")
        printHelp()
        sys.exit(0)
    if args:
        command=args.pop(0)
    else:
        command=""
    opts=args

    if not os.path.exists(PERSISTENCE):
        os.mkdir(PERSISTENCE)
    storage = yserial.Main("/".join([PERSISTENCE,projectName+".sqlite"]))
    try:
        debug("RESTORING DATASET...")
        dataset=NNTPDataset.restore()
        if not command:command="summarize"
    except:
        if DEBUG>1:traceback.print_exc()
        dataset=NNTPDataset()
        debug("CREATED NEW DATASET:",dataset)
        if not command:command="setup"
    if command=="summarize":
        dataset.summarize()
    elif command=="setup":
        debug("Starting setup of your project...")
        dataset.setup()
    elif command=="collect":
        dataset.download()
    elif command=="delete":
        tmp=input("Are you sure? (yes or no):")
        if tmp.lower()=="yes":
            os.remove("/".join([PERSISTENCE,projectName+".sqlite"]))
    elif command=="dump":
        if not opts:
            print("Supply a target filename")
        elif opts[0]=="-":
            dataset.dump(sys.stdout)
        else:
            dataset.dump(open(opts[0],"w"))
    elif command=="dumpcco":
        if not opts:
            print("Supply a target filename")
        elif opts[0]=="-":
            dataset.dumpcco(sys.stdout)
        else:
            dataset.dumpcco(open(opts[0],"w"))
    elif command=="dumpGEXF":
        if not opts:
            print("Supply a target filename")
        elif opts[0]=="-":
            dataset.dumpGEXF(sys.stdout,projectName)
        else:
            dataset.dumpGEXF(open(opts[0],"wb"),projectName)
    elif command=="dumpgeneric":
        dataset.dumpGeneric(reset=True)
    elif command in ["dumpfan","dumplax"]:
        reset=True
        opts, args = getopt.getopt(opts, "r", [])
        for o, a in opts:
            if o == "-r":
                reset = False
        if not args:
            print("Supply a target filename")
        elif args[0]=="-":
            fp=sys.stdout
        else:
            fp=open(args[0],"w")
        if command=="dumpfan":
            dataset.dumpFan(fp,reset=reset,projectName=projectName)
        else:
            dataset.dumpLax(fp,reset=reset,projectName=projectName)
    else:
        print("UNKNOWN COMMAND :",command)
        