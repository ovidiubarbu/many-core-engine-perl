Name:           perl-MCE
Version:        1.300
Release:        1%{?dist}
Summary:        Many-Core Engine for Perl. Provides parallel processing capabilities.
License:        CHECK(Distributable)
Group:          Development/Libraries
URL:            http://search.cpan.org/dist/MCE/
Source0:        http://search.cpan.org/CPAN/authors/id/M/MA/MARIOROY/MCE-%{version}.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildArch:      noarch
Requires:       perl(:MODULE_COMPAT_%(eval "`%{__perl} -V:version`"; echo $version))

%description
Many-core Engine (MCE) for Perl helps enable a new level of performance by
maximizing all available cores. MCE spawns a pool of workers and therefore
does not fork a new process per each element of data. Instead, MCE follows
a bank queuing model. Imagine the line being the data and bank-tellers the
parallel workers. MCE enhances that model by adding the ability to chunk
the next n elements from the input stream to the next available worker.

%prep
%setup -q -n MCE-%{version}

%build
%{__perl} Makefile.PL INSTALLDIRS=vendor
make %{?_smp_mflags}

%install
rm -rf $RPM_BUILD_ROOT

make pure_install PERL_INSTALL_ROOT=$RPM_BUILD_ROOT

find $RPM_BUILD_ROOT -type f -name .packlist -exec rm -f {} \;
find $RPM_BUILD_ROOT -depth -type d -exec rmdir {} 2>/dev/null \;

for f in examples/*
do
    if [ ! -d $f ]
    then
        install -D -p -m 0755 $f $RPM_BUILD_ROOT%{_datadir}/doc/%{name}-%{version}/$f
    fi
done

%{_fixperms} $RPM_BUILD_ROOT/*

%check
make test

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%doc CHANGES CREDITS LICENSE README TODO examples
%{perl_vendorlib}/*
%{_mandir}/man3/*

%changelog
* Mon Dec 31 2012 Mario Roy 1.300-1
- New methods...: chunk_size, restart_worker, task_id, task_wid, tmp_dir
- New options...: on_post_exit, on_post_run, sequence
- New examples..: forseq.pl, seq_demo.pl
- Overhaul to exit method
  Workers can exit or die without impacting the manager process
- Enabled executable bit for test files
- Removed localtime output in die and warn handlers
- All 3 delay options are consistent whether or not user_tasks is specified
- Removed logic around total_ended count -- replaced with new exit logic
- Code refactoring plus documentation updates
- Added LICENSE file
* Fri Dec 21 2012 Mario Roy 1.201-1
- Added MCE.pod -- moved documentation from MCE.pm to pod file
- Added missing use strict/warnings to test scripts
- Default to 1 for chunk_size and max_workers if not specified
- Test::More is not a requirement to run MCE, only for building
- Changed the format for the change log file
* Thu Dec 20 2012 Mario Roy 1.200-1
- Added new user_tasks option
- Added space between method name and left-paren for header lines in POD
- Remove not-needed BSD::Resource and forks inside BEGIN/INIT blocks
* Wed Dec 19 2012 Mario Roy 1.106-1
- Added t/pod-coverage.t
- Big overhaul of the MCE documentation -- all methods are documented
- Croak if method suited for a MCE worker is called by the main MCE process
- Croak if method suited for the main MCE process is called by a MCE worker
- Updated Makefile.PL to declare the minimum Perl version
* Sun Dec 16 2012 Mario Roy 1.105-1
- Completed code re-factoring
- Added t/pod.t
* Sun Nov 25 2012 Mario Roy 1.104-1
- Added 1 new example to MCE's Perl documentation
- Use module::method name versus constant symbol when calling _croak
- Croak if session directory is not writeable inside MCE::spawn
- Renamed _mce_id to _mce_sid (met to be spawn id actually)
- Re-calibrated maximum workers allowed
* Fri Nov 23 2012 Mario Roy 1.103-1
- Added writeable check on /dev/shm
- Croak if tmp dir is not writeable inside MCE::Signal::import
* Thu Nov 22 2012 Mario Roy 1.102-1
- Woohoot !!! MCE now passes with Perl 5.17.x
- Added Copying file -- same as in Perl
* Wed Nov 21 2012 Mario Roy 1.101-1
- Shifted white space to the left for code blocks inside documentation
* Wed Nov 21 2012 Mario Roy 1.100-1
- Completed optimization and shakeout for MCE's existing API
- File handles are cached when calling sendto and appending to a file
- The sendto method now supports multiple arguments -- see perldoc
- Added new option: flush_file
* Sat Nov 17 2012 Mario Roy 1.008-1
- Update on __DIE__ and __WARN__ handling in MCE. This addresses the
  unreferenced scalars seen in packaging logs at activestate.com for
  Perl under Windows: http://code.activestate.com/ppm/MCE/
- Update t/01_load_signal_arg.t -- added check for $ENV{TEMP}
  This fixes issue seen under Cygwin
* Thu Nov 15 2012 Mario Roy 1.007-1
- At last, the "Voila" release :)
- Small change to __DIE__ and __WARN__ signal handling for spawn method
* Thu Nov 15 2012 Mario Roy 1.006-1
- Added description section to MCE::Signal's Perl doc
- Do not set trap on __DIE__ and __WARN__ inside MCE::Signal
- Localized __DIE__ and __WARN__ handlers inside MCE instead
- Clarify the use of threads in documentation
* Tue Nov 13 2012 Mario Roy 1.005-1
- Removed underscore from package globals in MCE::Signal
- Optimized _worker_read_handle method in MCE
- Updated files under examples/tbray/
* Mon Nov 12 2012 Mario Roy 1.004-1
- Updated examples/mce_usage.readme
- Updated examples/wide_finder.pl
- Added examples/tbray/README
- Added examples/tbray/tbray_baseline1.pl
- Added examples/tbray/tbray_baseline2.pl
- Added examples/tbray/wf_mce1.pl
- Added examples/tbray/wf_mce2.pl
- Added examples/tbray/wf_mce3.pl (../wide_finder.pl moved here)
- Added examples/tbray/wf_mmap.pl
* Sat Nov 10 2012 Mario Roy 1.003-1
- Updated README
- Updated images/06_Shared_Sockets.gif
- Updated images/10_Scaling_Pings.gif
- Added   images/11_SNMP_Collection.gif
- Small update to MCE::Signal
* Thu Nov 08 2012 Mario Roy 1.002-1
- Renamed continue method to next 
* Wed Nov 07 2012 Mario Roy 1.001-1
- Added perl-MCE.spec to trunk
  http://code.google.com/p/many-core-engine-perl/source/browse/trunk/
- Added CREDITS 
- Added 3 new methods to MCE.pm: continue, last, and exit
- Both foreach & forchunk now call run(1, {...}) to auto-shutdown workers
* Tue Nov 06 2012 Joe Ogulin 1.000-1
- Specfile autogenerated by cpanspec 1.77
- Modified to include the examples and install into /usr/share/doc
