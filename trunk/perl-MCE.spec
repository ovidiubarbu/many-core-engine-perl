Name:           perl-MCE
Version:        1.406
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
* Tue Mar 12 2013 Mario Roy 1.406-1
- Added support for barrier synchronization (via new sync method).
- Addressed rounding errors with the sequence generator.
  The sequence option now follows a bank-teller queuing model when
  generating numbers. This applies to task 0 only. Other tasks can
  still specify sequence where numbers will be distributed equally
  among workers like before.
- Optimized the _worker_request_chunk private method.
- A take 2 on the matrix multiplication examples. This is better
  organized with updated README file to include the script running
  time in the results.
* Mon Mar 04 2013 Mario Roy 1.405-1
- Added strassen_pdl_t.pl in the event folks cannot make use of /dev/shm
  used by the strassen_pdl_s.pl example.
- Optimized the send method -- workers process immediately after receving
  data. Updated run times in README for the strassen examples.
- MCE no longer calls setpgrp by default as of MCE 1.405. There is only
  one reason to call setpgrp, but many reasons not to. The sole reason
  was for MCE to run correctly with Daemon::Control. If needed, one can
  pass the option to MCE::Signal qw(-setpgrp).
- Return void in the shutdown method (previously was returning $self).
- Tidy code inside sequence generator.
* Sun Feb 24 2013 Mario Roy 1.404-1
- Added sess_dir method 
- Completed work with matmult/* examples
  Added matmult_pdl_q.pl, removed strassen_pdl_h.pl
  Added strassen_pdl_o/p/q/r/s.pl 
  Added benchmark results from a 32-way box at the end of the readme
- Removed lines setting max limit for files/procs
* Sun Feb 17 2013 Mario Roy 1.403-1
- Wrap sub PDL::CLONE_SKIP into a no warnings 'redefine' block
  MCE now works with PDL::Parallel::threads without any warnings
- Added missing examples/matmult/matmult_pdl_n.pl to MANIFEST
- Refactored strassen examples, memory consumption reduced by > than 50%
- Added matmult_pdl_o.pl -- uses PDL::Parallel::threads to share matrices
- Added matmult_pdl_p.pl -- matrix b is read from shared memory, not mmap
- Added strassen_pdl_n.pl -- additional improvements to memory reduction
- Added strassen_pdl_h.pl -- shown running with 4 workers (half and half)
- Re-ran matrix multiplication examples and updated results in README file
- Added -no_setpgrp option to MCE::Signal.pm 
  Ctrl-C does not respond when running /usr/bin/time mce_script.pl
- Added undef $buffer in a couple of places within MCE.pm
- Added David Mertens and Adam Sjøgren to CREDITS 
- The send method now checks if sending > total workers after spawning
  not before
* Thu Feb 14 2013 Mario Roy 1.402-1
- Updated matrix multiplication examples including README
- Added examples/matmult/matmult_pdl_n.pl
* Mon Feb 12 2013 Mario Roy 1.401-1
- Added sub PDL::CLONE_SKIP { 1 } to MCE.pm. Running PDL + MCE threads no
  longer crashes during exiting.
- Updated matrix multiplication examples. All examples now work under the
  windows environment no matter if threading or forking. Unix is stable as
  well if wanting to use PDL + MCE and use_threads => 1 or 0.
- Added benchmark results for 2048x2048, 4096x4096, and 8192x8192 to the
  README file under examples/matmult/
- Updated documentation
* Mon Feb 11 2013 Mario Roy 1.400-1
- 1.400 release.
