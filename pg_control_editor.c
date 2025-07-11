/*
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 */ 

#define FRONTEND 1

#define PG_CONTROL_FILE_PATH_SIZE		8192

#include "postgres.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "fe_utils/option_utils.h"
#include "getopt_long.h"
#include "pg_getopt.h"

#include "common/logging.h"
#include "common/controldata_utils.h"
#include "access/xlog_internal.h"
#include "access/multixact.h"

static void usage(void);
static bool read_controlfile(const char*);
static void	make_datadir_out_if_not_exists(const char*);

static const char *progname;
static ControlFileData ControlFile; /* pg_control values */
static char* DataDirOut = NULL;
static char* DataDirIn = NULL;
static bool guessed = false;	/* T if we had to guess at any values */

static Oid	set_oid = 0;
static TransactionId set_xid = 0;
static MultiXactId set_mxid = 0;
static MultiXactOffset set_mxoff = (MultiXactOffset) -1;
static TimeLineID minXlogTli = 0;
static TransactionId set_oldest_commit_ts_xid = 0;
static TransactionId set_newest_commit_ts_xid = 0;
static TransactionId set_oldest_xid = 0;
static uint32 set_xid_epoch = (uint32) -1;
static int	set_wal_segsize = 0;
static int	WalSegSz;
static XLogSegNo minXlogSegNo = 0;

int
main(int argc, char *argv[])
{
	int	c;
	static struct option long_options[] = {
		{"pgdata-in", required_argument, NULL, 'D'},
		{"pgdata-out", required_argument, NULL, 'd'},
		{"commit-timestamp-ids", required_argument, NULL, 'c'},
		{"epoch", required_argument, NULL, 'e'},
		{"next-wal-file", required_argument, NULL, 'l'},
		{"multixact-ids", required_argument, NULL, 'm'},
		{"next-oid", required_argument, NULL, 'o'},
		{"multixact-offset", required_argument, NULL, 'O'},
		{"oldest-transaction-id", required_argument, NULL, 'u'},
		{"next-transaction-id", required_argument, NULL, 'x'},
		{"wal-segsize", required_argument, NULL, 1},
		{NULL, 0, NULL, 0}
	};
	char	   *endptr;
	char	   *endptr2;
	MultiXactId set_oldestmxid = 0;
	char	   *log_fname = NULL;

	pg_logging_init(argv[0]);
	progname = get_progname(argv[0]);
	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			usage();
			exit(0);
		}
	}

	while ((c = getopt_long(argc, argv, "D:d:o:x:m:O:c:e:l:u:1:", long_options, NULL)) != -1)
	{
		switch (c)
		{
			case 'D':
				DataDirIn = optarg;
				break;

			case 'd':
				DataDirOut = optarg;
				break;

			case 'o':
				errno = 0;
				set_oid = strtoul(optarg, &endptr, 0);
				if (endptr == optarg || *endptr != '\0' || errno != 0)
				{
					pg_log_error("invalid argument for option %s", "-o");
					pg_log_error_hint("Try \"%s --help\" for more information.", progname);
					exit(1);
				}
				if (set_oid == 0)
					pg_fatal("OID (-o) must not be 0");
				break;

			case 'x':
				errno = 0;
				set_xid = strtoul(optarg, &endptr, 0);
				if (endptr == optarg || *endptr != '\0' || errno != 0)
				{
					pg_log_error("invalid argument for option %s", "-x");
					pg_log_error_hint("Try \"%s --help\" for more information.", progname);
					exit(1);
				}
				if (!TransactionIdIsNormal(set_xid))
					pg_fatal("transaction ID (-x) must be greater than or equal to %u", FirstNormalTransactionId);
				break;

			case 'm':
				errno = 0;
				set_mxid = strtoul(optarg, &endptr, 0);
				if (endptr == optarg || *endptr != ',' || errno != 0)
				{
					pg_log_error("invalid argument for option %s", "-m");
					pg_log_error_hint("Try \"%s --help\" for more information.", progname);
					exit(1);
				}

				set_oldestmxid = strtoul(endptr + 1, &endptr2, 0);
				if (endptr2 == endptr + 1 || *endptr2 != '\0' || errno != 0)
				{
					pg_log_error("invalid argument for option %s", "-m");
					pg_log_error_hint("Try \"%s --help\" for more information.", progname);
					exit(1);
				}
				if (set_mxid == 0)
					pg_fatal("multitransaction ID (-m) must not be 0");

				/*
				 * XXX It'd be nice to have more sanity checks here, e.g. so
				 * that oldest is not wrapped around w.r.t. nextMulti.
				 */
				if (set_oldestmxid == 0)
					pg_fatal("oldest multitransaction ID (-m) must not be 0");
				break;

			case 'O':
				errno = 0;
				set_mxoff = strtoul(optarg, &endptr, 0);
				if (endptr == optarg || *endptr != '\0' || errno != 0)
				{
					pg_log_error("invalid argument for option %s", "-O");
					pg_log_error_hint("Try \"%s --help\" for more information.", progname);
					exit(1);
				}
				if (set_mxoff == -1)
					pg_fatal("multitransaction offset (-O) must not be -1");
				break;

			case 'c':
				errno = 0;
				set_oldest_commit_ts_xid = strtoul(optarg, &endptr, 0);
				if (endptr == optarg || *endptr != ',' || errno != 0)
				{
					pg_log_error("invalid argument for option %s", "-c");
					pg_log_error_hint("Try \"%s --help\" for more information.", progname);
					exit(1);
				}
				set_newest_commit_ts_xid = strtoul(endptr + 1, &endptr2, 0);
				if (endptr2 == endptr + 1 || *endptr2 != '\0' || errno != 0)
				{
					pg_log_error("invalid argument for option %s", "-c");
					pg_log_error_hint("Try \"%s --help\" for more information.", progname);
					exit(1);
				}

				if (set_oldest_commit_ts_xid < FirstNormalTransactionId &&
					set_oldest_commit_ts_xid != InvalidTransactionId)
					pg_fatal("transaction ID (-c) must be either %u or greater than or equal to %u", InvalidTransactionId, FirstNormalTransactionId);

				if (set_newest_commit_ts_xid < FirstNormalTransactionId &&
					set_newest_commit_ts_xid != InvalidTransactionId)
					pg_fatal("transaction ID (-c) must be either %u or greater than or equal to %u", InvalidTransactionId, FirstNormalTransactionId);
				break;

			case 'e':
				errno = 0;
				set_xid_epoch = strtoul(optarg, &endptr, 0);
				if (endptr == optarg || *endptr != '\0' || errno != 0)
				{
					/*------
					  translator: the second %s is a command line argument (-e, etc) */
					pg_log_error("invalid argument for option %s", "-e");
					pg_log_error_hint("Try \"%s --help\" for more information.", progname);
					exit(1);
				}
				if (set_xid_epoch == -1)
					pg_fatal("transaction ID epoch (-e) must not be -1");
				break;

			case 'l':
				if (strspn(optarg, "01234567890ABCDEFabcdef") != XLOG_FNAME_LEN)
				{
					pg_log_error("invalid argument for option %s", "-l");
					pg_log_error_hint("Try \"%s --help\" for more information.", progname);
					exit(1);
				}

				/*
				 * XLogFromFileName requires wal segment size which is not yet
				 * set. Hence wal details are set later on.
				 */
				log_fname = pg_strdup(optarg);
				break;

			case 'u':
				errno = 0;
				set_oldest_xid = strtoul(optarg, &endptr, 0);
				if (endptr == optarg || *endptr != '\0' || errno != 0)
				{
					pg_log_error("invalid argument for option %s", "-u");
					pg_log_error_hint("Try \"%s --help\" for more information.", progname);
					exit(1);
				}
				if (!TransactionIdIsNormal(set_oldest_xid))
					pg_fatal("oldest transaction ID (-u) must be greater than or equal to %u", FirstNormalTransactionId);
				break;

			case 1:
				{
					int			wal_segsize_mb;

					if (!option_parse_int(optarg, "--wal-segsize", 1, 1024, &wal_segsize_mb))
						exit(1);
					set_wal_segsize = wal_segsize_mb * 1024 * 1024;
					if (!IsValidWalSegSize(set_wal_segsize))
						pg_fatal("argument of %s must be a power of two between 1 and 1024", "--wal-segsize");
					break;
				}

			default:
				/* getopt_long already emitted a complaint */
				pg_log_error_hint("Try \"%s --help\" for more information.", progname);
				exit(1);
		}
	}

	/* Complain if any arguments remain */
	if (optind < argc)
	{
		pg_log_error("too many command-line arguments (first is \"%s\")",
					 argv[optind]);
		pg_log_error_hint("Try \"%s --help\" for more information.", progname);
		exit(1);
	}

	if (DataDirIn == NULL || DataDirOut == NULL)
	{
		pg_log_error("Both input/output data directory should be specified.");
		pg_log_error_hint("Try \"%s --help\" for more information.", progname);
		exit(1);
	}

	if (! read_controlfile(DataDirIn))
	{
		pg_log_error("Could not read control file from the input directory \"%s\"",
		             DataDirIn);
		exit(1);
	}

	if (set_wal_segsize != 0)
		WalSegSz = set_wal_segsize;
	else
		WalSegSz = ControlFile.xlog_seg_size;

	if (log_fname != NULL)
		XLogFromFileName(log_fname, &minXlogTli, &minXlogSegNo, WalSegSz);

	if (set_oid != 0)
		ControlFile.checkPointCopy.nextOid = set_oid;

	if (set_xid != 0)
		ControlFile.checkPointCopy.nextXid =
			FullTransactionIdFromEpochAndXid(EpochFromFullTransactionId(ControlFile.checkPointCopy.nextXid),
											 set_xid);
	if (set_mxid != 0)
	{
		ControlFile.checkPointCopy.nextMulti = set_mxid;

		ControlFile.checkPointCopy.oldestMulti = set_oldestmxid;
		if (ControlFile.checkPointCopy.oldestMulti < FirstMultiXactId)
			ControlFile.checkPointCopy.oldestMulti += FirstMultiXactId;
		ControlFile.checkPointCopy.oldestMultiDB = InvalidOid;
	}

	if (set_mxoff != -1)
		ControlFile.checkPointCopy.nextMultiOffset = set_mxoff;

	if (minXlogTli > ControlFile.checkPointCopy.ThisTimeLineID)
	{
		ControlFile.checkPointCopy.ThisTimeLineID = minXlogTli;
		ControlFile.checkPointCopy.PrevTimeLineID = minXlogTli;
	}

	if (set_oldest_commit_ts_xid != 0)
		ControlFile.checkPointCopy.oldestCommitTsXid = set_oldest_commit_ts_xid;
	if (set_newest_commit_ts_xid != 0)
		ControlFile.checkPointCopy.newestCommitTsXid = set_newest_commit_ts_xid;

	if (set_xid_epoch != -1)
		ControlFile.checkPointCopy.nextXid =
			FullTransactionIdFromEpochAndXid(set_xid_epoch,
											 XidFromFullTransactionId(ControlFile.checkPointCopy.nextXid));

	if (set_oldest_xid != 0)
	{
		ControlFile.checkPointCopy.oldestXid = set_oldest_xid;
		ControlFile.checkPointCopy.oldestXidDB = InvalidOid;
	}

	if (set_wal_segsize != 0)
		ControlFile.xlog_seg_size = WalSegSz;

	make_datadir_out_if_not_exists(DataDirOut);

	update_controlfile(DataDirOut, &ControlFile, false);
	return 0;
}


/*
 * Try to read the existing pg_control file.
 *
 * This routine is also responsible for updating old pg_control versions
 * to the current format.  (Currently we don't do anything of the sort.)
 */
static bool
read_controlfile(const char* pgdata_in)
{
	int			fd;
	int			len;
	char	    filepath[PG_CONTROL_FILE_PATH_SIZE] = {0};
	char	    *buffer;
	pg_crc32c	crc;

	snprintf(filepath, PG_CONTROL_FILE_PATH_SIZE - 1, "%s/%s", pgdata_in, XLOG_CONTROL_FILE);


	if ((fd = open(filepath, O_RDONLY | PG_BINARY, 0)) < 0)
	{
		/*
		 * If pg_control is not there at all, or we can't read it, the odds
		 * are we've been handed a bad DataDir path, so give up. User can do
		 * "touch pg_control" to force us to proceed.
		 */
		pg_log_error("could not open file \"%s\" for reading: %m",
					 XLOG_CONTROL_FILE);
		if (errno == ENOENT)
			pg_log_error_hint("If you are sure the data directory path is correct, execute\n"
							  "  touch %s\n"
							  "and try again.",
							  XLOG_CONTROL_FILE);
		exit(1);
	}

	/* Use malloc to ensure we have a maxaligned buffer */
	buffer = (char *) pg_malloc(PG_CONTROL_FILE_SIZE);

	len = read(fd, buffer, PG_CONTROL_FILE_SIZE);
	if (len < 0)
		pg_fatal("could not read file \"%s\": %m", XLOG_CONTROL_FILE);
	close(fd);

	if (len >= sizeof(ControlFileData) &&
		((ControlFileData *) buffer)->pg_control_version == PG_CONTROL_VERSION)
	{
		/* Check the CRC. */
		INIT_CRC32C(crc);
		COMP_CRC32C(crc,
					buffer,
					offsetof(ControlFileData, crc));
		FIN_CRC32C(crc);

		if (!EQ_CRC32C(crc, ((ControlFileData *) buffer)->crc))
		{
			/* We will use the data but treat it as guessed. */
			pg_log_warning("pg_control exists but has invalid CRC; proceed with caution");
			guessed = true;
		}

		memcpy(&ControlFile, buffer, sizeof(ControlFile));

		/* return false if WAL segment size is not valid */
		if (!IsValidWalSegSize(ControlFile.xlog_seg_size))
		{
			pg_log_warning(ngettext("pg_control specifies invalid WAL segment size (%d byte); proceed with caution",
									"pg_control specifies invalid WAL segment size (%d bytes); proceed with caution",
									ControlFile.xlog_seg_size),
						   ControlFile.xlog_seg_size);
			return false;
		}
		return true;
	}

	/* Looks like it's a mess. */
	pg_log_warning("pg_control exists but is broken or wrong version; ignoring it");
	return false;
}


static void
make_datadir_out_if_not_exists(const char* pgdata_out)
{
	char filepath[PG_CONTROL_FILE_PATH_SIZE] = {0};
	int fd;

	if (mkdir(pgdata_out, 0755)) {
		if (errno != EEXIST) {
			pg_log_error("output pgdata \"%s\" doesn't exists and cannot mkdir: %m",
		                 pgdata_out);
			exit(1);
		}
	}

	snprintf(filepath, PG_CONTROL_FILE_PATH_SIZE - 1, "%s/global", pgdata_out);
	if (mkdir(filepath, 0755)) {
		if (errno != EEXIST) {
			pg_log_error("directory \"%s\" doesn't exists and cannot mkdir: %m",
			             filepath);
			exit(1);
		}
	}

	snprintf(filepath, PG_CONTROL_FILE_PATH_SIZE - 1, "%s/%s",
	         pgdata_out, XLOG_CONTROL_FILE);
	if ((fd = open(filepath, O_EXCL | O_CREAT, 0644)) == -1) {
		if (errno != EEXIST) {
			pg_log_error("file \"%s\" doesn't exists and cannot create: %m",
			             filepath);
			exit(1);
		}
	} else {
		close(fd);
	}
}


static void
usage(void)
{
	printf(_("%s is a tool to modify a control file.\n\n"), progname);
	printf(_("Usage:\n"));
	printf(_(" -D, --pgdata-in=DATADIR   input data directory\n"));
	printf(_(" -d, --pgdata-out=DATADIR  output data directory\n"));
	printf(_(" -?, --help                show this help, then exit\n"));
	printf(_("\nOptions to override control file values:\n"));
	printf(_("  -c, --commit-timestamp-ids=XID,XID\n"
			 "                                   set oldest and newest transactions bearing\n"
			 "                                   commit timestamp (zero means no change)\n"));
	printf(_("  -e, --epoch=XIDEPOCH             set next transaction ID epoch\n"));
	printf(_("  -l, --next-wal-file=WALFILE      set minimum starting location for new WAL\n"));
	printf(_("  -m, --multixact-ids=MXID,MXID    set next and oldest multitransaction ID\n"));
	printf(_("  -o, --next-oid=OID               set next OID\n"));
	printf(_("  -O, --multixact-offset=OFFSET    set next multitransaction offset\n"));
	printf(_("  -u, --oldest-transaction-id=XID  set oldest transaction ID\n"));
	printf(_("  -x, --next-transaction-id=XID    set next transaction ID\n"));
	printf(_("      --wal-segsize=SIZE           size of WAL segments, in megabytes\n"));
}

