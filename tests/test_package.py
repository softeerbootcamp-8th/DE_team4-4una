from de4 import main


def test_main(capsys) -> None:
    main()

    assert capsys.readouterr().out == "Hello from 4una!\n"
