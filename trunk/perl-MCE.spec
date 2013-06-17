Name:           perl-MCE
Version:        1.499
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

Both chunking and input are optional in MCE. One can simply use MCE to
have many workers run in parallel.

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
* Mon Jun 17 2013 Mario Roy 1.499-1
- 1.499 Release.
