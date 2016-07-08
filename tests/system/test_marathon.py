"""Marathon acceptance tests for DC/OS."""

from shakedown import *

import pytest

PACKAGE_NAME = 'marathon'
DCOS_SERVICE_URL = dcos_service_url(PACKAGE_NAME)
WAIT_TIME_IN_SECS = 300

@pytest.mark.sanity
def test_install_marathon():
    """Install the Marathon package for DC/OS.
    """

    # Install
    print("Installing...")
    install_package_and_wait(PACKAGE_NAME)
    assert package_installed(PACKAGE_NAME), 'Package failed to install'

    end_time = time.time() + WAIT_TIME_IN_SECS
    found = False
    while time.time() < end_time:
        found = get_service(PACKAGE_NAME) is not None
        if found and service_healthy(PACKAGE_NAME):
            break
        time.sleep(1)

    assert found, 'Service did not register with DCOS'
    print("service found...")

    #Uninstall
    uninstall_package_and_wait(PACKAGE_NAME)
    assert not package_installed(PACKAGE_NAME), 'Package failed to uninstall'
    print("service uninstalled...")

    # Reinstall
    install_package_and_wait(PACKAGE_NAME)
    assert package_installed(PACKAGE_NAME), 'Package failed to reinstall'

    try:
        install_package(PACKAGE_NAME)
    except Exception as e:
        print("Exception raised")
        pass
    else:
        # Exception is not raised -> exit code was 0
        assert False, "Error: CLI returns 0 when asked to install Marathon"

    # pytest teardown do not seem to be working
    print("teardown...")
    uninstall_package(PACKAGE_NAME)
    time.sleep(5)
    run_command_on_master("docker run mesosphere/janitor /janitor.py -z universe/marathon-user")