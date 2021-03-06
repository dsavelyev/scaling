=======
scaling
=======
scaling - программа для автоматического запуска программ на удаленных
суперкомпьютерных системах во многих экземплярах, сбора их вывода и выделения
из него данных об их выполнении.

Установка
---------
``python3 setup.py install`` или ``pip3 install .``

Использование
-------------
Для проведения запуска с помощью scaling требуются конфигурационные файлы:

- машины, на которой производится запуск
- среды запуска
- программы (ее аргументов, входных и выходных файлов)
- множества параметров

Кроме последнего, эти файлы пишутся в формате `TOML
<https://github.com/toml-lang/toml/blob/v0.4.0/README.md>`_.

Конфигурация машины
-------------------
Конфигурация машины - это имя хоста и имя пользователя. Пример::

    host = "somecluster.example.com"
    user = "user"

Поддерживается только авторизация по SSH-ключам, которые должны быть доступны
из SSH-агента или в $HOME/.ssh, $HOME/ssh.

Конфигурация среды запуска
--------------------------
В конфигурации запуска задаются пути к скриптам, лежащим на удаленной машине,
которые отвечают за:

#. постановку задачи на счет (``scaling-start``),
#. опрос статуса задач (``scaling-poll``),
#. отмену задач (``scaling-cancel``).

Также задаются параметры для первого скрипта и выходные файлы, которые
генерируются этой средой вне зависимости от выполняемой программы.

Переменные конфигурации:

- machine - путь к файлу конф. машины
- start_cmd - скрипт запуска
- poll_cmd - скрипт опроса
- cancel_cmd - скрипт отмены
- base_dir - каталог на удаленной машине, в котором будут создаваться:

  - рабочие подкаталоги для задач
  - файлы вывода

- param_order - порядок параметров к скрипту запуска
- params - таблица параметров, вида "имя = тип" (о типах ниже)
- out_file_specs - массив спецификаций выходных файлов (о них ниже)

В каталоге remote лежат скрипты для менеджеров задач SLURM и LoadLeveler, для
систем суперкомпьютерного комплекса МГУ "Ломоносов", "Ломоносов-2" и Blue
Gene/P. Нужно загрузить содержимое подкаталога common и соответствующих
подкаталогов в каталогах cluster и jobmanager в один и тот же каталог *dir*, чтобы
получить скрипты scaling-{start,poll,cancel} в *dir*/bin. На "Ломоносовых"
каталог *dir* должен располагаться в разделе _scratch.

Т.е. для "Ломоносовых" в *dir* нужно загрузить содержимое следующих каталогов:

- remote/common
- remote/jobmanager/slurm
- remote/cluster/lomonosov

Для Blue Gene:

- remote/common
- remote/jobmanager/loadleveler
- remote/cluster/bgp

На Blue Gene
нужно также откомпилировать программу llsubmit-with-monitor, лежащую в
remote/jobmanager/loadleveler/src, и поместить ее в *dir*/bin.

Кроме того, (только на "Ломоносовых" и только с Open MPI) можно откомпилировать
библиотеку mpiperf (remote/common/src/mpiperf) и прописать путь в каталог с ней
в переменной SODIR в *dir*/lib/hooks.bash (в каталоге, куда распакованы скрипты).
Эта библиотека замеряет время выполнения MPI-программы от первого MPI_Init до
последнего MPI_Finalize и записывает в ``__mpiperf.txt`` (выходная переменная ``mpitime``).

Шаблоны конфигурации для "Ломоносовых" и Blue Gene лежат в examples. В них
нужно подставить получившиеся полные пути к скриптам start, poll и cancel в
параметры start_cmd, poll_cmd и cancel_cmd соответственно.

Спецификации выходных файлов
----------------------------
Это таблицы, имеющие элементами:

- name - шаблон имени файла (glob, `fnmatch <https://docs.python.org/3/library/fnmatch.html>`_)
- outputspecs - массив спецификаций вывода, каждая из которых содержит:

  - regex - регулярное выражение (о синтаксисе ниже),
  - vartypes - таблица типов переменных.

::

    [[out_file_specs]]
    name = "somefile*"

        [[out_file_specs.outputspecs]]
        regex = "t1=%{FLOAT:time1} t2=%{FLOAT:time2}"

            [[out_file_specs.outputspecs.vartypes]]
            time1 = "float"
            time2 = "float"

        [[out_file_specs.outputspecs]]
        ...

     [[out_file_specs]]
     ...

Конфигурация программы
----------------------
- args - массив аргументов командной строки с подстановками вида $VAR, где VAR
  \- переменная,
- params - таблица типов переменных,
- stdout - массив спецификаций вывода (см. выше),
- out_file_specs - массив спецификаций выходных файлов (см. выше),
- in_file_specs - массив спецификаций входных файлов (см. ниже).

Пример (бенчмарк HPL)::

    args = []

    [params]
    matsize = "int"
    blocksize = "int"
    p = "int"
    q = "int"

    [[stdout]]
    regex = '''
    T/V                N    NB     P     Q               Time
    Gflops
    --------------------------------------------------------------------------------
    (\S+\s+){5}%{FLOAT:time}\s+%{FLOAT:gflops}'''

    [stdout.vartypes]
    time = "float"
    gflops = "float"

    [[in_file_specs]]
    name = "HPL.dat"
    template = "HPL.dat.template"

Спецификации входных файлов
---------------------------
- name - имя файла, который создается на удаленной машине
- template - имя файла-шаблона

Пример файла-шаблона (HPL.dat.template)::

    HPLinpack benchmark input file
    Innovative Computing Laboratory, University of Tennessee
    HPL.out      output file name (if any)
    6            device out (6=stdout,7=stderr,file)
    1            # of problems sizes (N)
    $matsize     Ns
    1            # of NBs
    $blocksize   NBs
    0            PMAP process mapping (0=Row-,1=Column-major)
    1            # of process grids (P x Q)
    $p           Ps
    $q           Qs
    16.0         threshold
    1            # of panel fact
    0            PFACTs (0=left, 1=Crout, 2=Right)
    1            # of recursive stopping criterium
    2            NBMINs (>= 1)
    1            # of panels in recursion
    2            NDIVs
    1            # of recursive panel fact.
    0            RFACTs (0=left, 1=Crout, 2=Right)
    1            # of broadcast
    0            BCASTs (0=1rg,1=1rM,2=2rg,3=2rM,4=Lng,5=LnM)
    1            # of lookahead depth
    0            DEPTHs (>=0)
    2            SWAP (0=bin-exch,1=long,2=mix)
    64           swapping threshold
    0            L1 in (0=transposed,1=no-transposed) form
    0            U  in (0=transposed,1=no-transposed) form
    1            Equilibration (0=no,1=yes)
    8            memory alignment in double (> 0)

Регулярные выражения
--------------------
Синтаксис:

- `re <https://docs.python.org/3/library/re.html>`_
- плюс `regex <https://pypi.org/project/regex/>`_
- плюс синтаксис, вдохновленный Logstash Grok.

Синтаксис Grok: ``%{ШАБЛОН:имя}``, где ШАБЛОН - это INT, FLOAT или QUOTEDSTRING, а
имя - имя выходного параметра. Сам символ % пишется как %%. Также можно
задавать именованные группы: ``(?P<имя>подвыражение)``.

Типы параметров
---------------
int, float, str - как входные, так и выходные.

Спецификации входных параметров
-------------------------------
Общий вид::

    var11, ..., var1N: expr1;
    ...
    varM1, ..., varMN: exprM;

Где expr - выражения, имеющие тип скалярный, списка или списка из списков.
Каждое выражение сопоставляется переменной или списку переменных, причем в
последнем случае выражение должно иметь тип списка из списков.

Множество параметров формируется следующим образом: после вычисления каждого
значения выражения,

- если его значение скалярное, то оно присваивается сопоставленной
  переменной, которая должна быть единственной;
- если выражение имеет тип списка из скалярных значений, то поочередно
  каждое значение из списка присваивается переменной, которая должна
  быть единственной;
- если выражение имеет тип списка из списков, то поочередно каждый
  подсписок присваивается поэлементно переменным из списка
  переменных, сопоставленных данному выражению, причем длина каждого
  такого подсписка должна равняться длине списка переменных.

Значения выражений могут зависеть от значений переменных. В итоге
описанное спецификацией множество состоит из всех комбинаций
значений переменных, которые она допускает.

Пример::

    type: "ompi";
    modules: "openmpi mkl";
    partition: "test";
    preload: 0;

    ntasks: range(14, 168, 14);
    ntasks_per_node: 14;

    matsize: range(1000, 20000, 1000);

    blocksize: 100;

    p, q: multipartitions(ntasks, 2, 1);

    iter: range(3);

ntasks - в диапазоне [14, 168] с шагом 14, matsize - в диапазоне [1000, 20000]
с шагом 1000, p, q перебирают все разложения ntasks на два множителя, включая
разложения вида m x 1.

Операторы: +, -, \*, /, % (остаток), ^ (степень). Причем / дает int, если и
только если оба операнда int, и float в других случаях, а % работает, как в
Python 3 (для чисел).

Функции:

range(start, stop[, step]), range(stop): список целых чисел из диапазона [start, stop] с шагом step, либо [1, stop] с шагом 1

isqrt_floor(x): квадратный корень из x с округлением к меньшему

isqrt_ceil(x):
квадратный корень из x с округлением к бесконечности

sqrt(x):
квадратный корень из х (вещественный)

log(x, base):
логарифм x по основанию base (вещественный):

ilog_floor(x, base):
логарифм x по основанию base с округлением к меньшему

ilog_ceil(x, base):
логарифм x по основанию base с округлением к бесконечности

floor(x):
округление x к меньшему

ceil(x):
округление x к большему

round(x):
округление x к ближайшему целому

int(x):
округление x к нулю

float(x):
преобразование x к вещественному виду

multipartitions(x, count[, incl_ones]):
список из списков, каждый из которых является разложением x на count
множителей (включая множители «1», если incl_ones = 1)

zip(list1, list2, …, listn):
список из списков, где список под номером i содержит все i-тые элементы переданных
списков

concat(list1, list2, ..., listn):
конкатенация списков

Запуск
------
``scaling genparams -l <конфигурация среды запуска> -p <конф. программы> -s
<конф. множества> -e <полный путь к исполняемому файлу на удаленной машине> -o <выход: файл описания
эксперимента>``

Файл описания эксперимента будет содержать для каждого запуска:

- параметры
- аргументы командной строки
- содержимое входных файлов

``scaling launch -i <описание эксперимента> [-t var value -t var value...] -o
<файл результатов>``

Параметром -t можно ограничить суммарное значение определенных параметров —
например, числа вычислительных узлов — в любой конкретный момент времени.
Также можно ограничить таким образом число задач в очереди - для этого нужно
прописать параметр, например, ``num_jobs: 1``, а затем ограничить его: ``-t
num_jobs 10``.

Файл результатов - промежуточный, он содержит финальные состояния задач, их
идентификаторы и пути к рабочим каталогам.

``scaling getoutputs -l <описание эксперимента> -r <файл результатов> -o
<выход: CSV>``

CSV-файл будет содержать по строке на запуск, в каждой строке - входные и
выходные параметры. Отсутствующие по каким-то причинам (задача завершилась с
ошибкой, выходной файл не найден) параметры становятся пустыми (например, в
``1,2,3,4,5,,7`` отсутствует 6-й параметр).

Конфигурации среды для конкретных суперкомпьютеров
--------------------------------------------------
Параметры для "Ломоносовых":

- type - тип задачи, ompi или impi
- modules - список подгружаемых модулей, разделенный пробелами
- partition - раздел
- ntasks - число процессов
- ntasks_per_node - число процессов на узел
- preload - 1 или 0, подгружать ли mpiperf

Выходная переменная ``mpitime`` - время от первого ``MPI_Init`` до последнего
``MPI_Finalize`` (только при preload=1).

Для Blue Gene:

- ntasks - число процессов
- mode - smp, dual или vn

Шаблоны соответствующих конфигураций среды лежат в examples.
